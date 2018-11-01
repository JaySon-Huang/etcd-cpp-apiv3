#include <etcd/v3/AsyncWatchAction.hpp>
#include <etcd/v3/action_constants.hpp>


using etcdserverpb::RangeRequest;
using etcdserverpb::RangeResponse;
using etcdserverpb::WatchCreateRequest;


etcdv3::AsyncWatchAction::AsyncWatchAction(etcdv3::ActionParameters param)
  : etcdv3::Action(std::move(param))
  , revision(parameters.revision)
  , isCancelled(false)
  , stream(parameters.watch_stub->AsyncWatch(&context, &cq_, (void*)"create"))
{
  WatchRequest watch_req;
  {
    WatchCreateRequest watch_create_req;
    watch_create_req.set_key(parameters.key);
    // TODO: set_prev_kv - as param from outside
    watch_create_req.set_prev_kv(true);
    watch_create_req.set_start_revision(parameters.revision);
    //watch_create_req.set_progress_notify(true);

    if(parameters.withPrefix)
    {
      std::string range_end(parameters.key);
      int ascii = static_cast<int>(range_end[range_end.length() - 1]);
      range_end.back() = ascii + 1;
      watch_create_req.set_range_end(std::move(range_end));
    }

    *(watch_req.mutable_create_request()) = std::move(watch_create_req);
  }

  void * got_tag;
  bool ok = false;
  if (cq_.Next(&got_tag, &ok) && ok && got_tag == (void*)"create")
  {
    printResponse("AFTER CREATE:");
    stream->Write(watch_req, (void*)"write");
    ok = false;
    if (cq_.Next(&got_tag, &ok) && ok && got_tag == (void*)"write")
    {
      printResponse("AFTER WRITE:");
      stream->Read(&response, (void*)this);
      ok = false;
      if (cq_.Next(&got_tag, &ok) && ok && got_tag == (void*)this)
      {
        printResponse("AFTER FIRST READ:");
        // parse create response
        storeLastRevision();
        if (response.compact_revision() > 0)
        {
          throw async_watch_error("watch has not been created on etcd server due to trying to watch compacted revision");
        }
        if (response.canceled())
        {
          throw async_watch_error("watch has been cancelled when creating on etcd server");
        }
        if (!response.created())
        {
          throw async_watch_error("watch had not been created on etcd server");
        }
        stream->Read(&response, (void*)this);
      }
      else
      {
        cq_.Shutdown();
        throw async_watch_error("no answer from etcd server");
      }
    }
    else
    {
      cq_.Shutdown();
      throw async_watch_error("cannot send watch create request to etcd server");
    }
  }
  else
  {
    cq_.Shutdown();
    throw async_watch_error("cannot call gRPC method for watch");
  }
}

// TODO: throw in case of !ok
void etcdv3::AsyncWatchAction::waitForResponse() 
{
  void* got_tag;
  bool ok = false;
  
  while(cq_.Next(&got_tag, &ok))
  {
    // if not ok or unknown tag received (or write done), break watch
    if (!ok || got_tag != (void*)this)
    {
      cq_.Shutdown();
      break;
    }

    // ok and got tag is for read
    if(response.events_size())
    {
      stream->WritesDone((void*)"writes done");
    }
    else
    {
      stream->Read(&response, (void*)this);
    }
  }
}

// TODO: maybe rework with WatchCancelResponse
void etcdv3::AsyncWatchAction::cancelWatch()
{
  if (!isCancelled)
  {
    //check if cq_ is ok
    isCancelled = true;
    void * got_tag;
    bool ok = false;
    gpr_timespec deadline;
    deadline.clock_type = GPR_TIMESPAN;
    deadline.tv_sec = 0;
    deadline.tv_nsec = 10000000;
    if (cq_.AsyncNext(&got_tag, &ok, deadline) != CompletionQueue::SHUTDOWN)
    {
      stream->WritesDone((void*)"writes done");
    }
  }
}

int64_t etcdv3::AsyncWatchAction::lastRevision() const
{
  return revision;
}

void etcdv3::AsyncWatchAction::storeLastRevision()
{
  if (response.has_header() && response.header().revision() > 0)
  {
    revision = response.header().revision();
  }
}

// TODO: throw in case of !ok
void etcdv3::AsyncWatchAction::waitForResponse(watch_callback const & callback)
{
  void * got_tag;
  bool ok = false;

  while (cq_.Next(&got_tag, &ok))
  {
    // if not ok or unknown tag received (or write done), break watch
    if (!ok || got_tag != (void*)this)
    {
      cq_.Shutdown();
      break;
    }

    storeLastRevision();

    // if ok and read done, call callback (only if reply has events) and just read again
    if (response.events_size())
    {
      // callback is responsible for errors handling, all raised exceptions will be ignored
      try
      {
        callback(etcd::Response(ParseResponse()));
      }
      catch (...)
      {}
    }
    stream->Read(&response, (void *)this);
  }
}

etcdv3::AsyncWatchResponse etcdv3::AsyncWatchAction::ParseResponse()
{
  if (!status.ok())
  {
    return AsyncWatchResponse(status.error_code(), status.error_message());
  }

  return AsyncWatchResponse(response);
}
