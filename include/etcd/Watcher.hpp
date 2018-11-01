#ifndef __ETCD_WATCHER_HPP__
#define __ETCD_WATCHER_HPP__

#include <string>
#include <etcd/Response.hpp>
#include <etcd/v3/AsyncWatchAction.hpp>

#include <grpc++/grpc++.h>

#include <map>
// TODO: which one is needed?
#include <exception>
#include <stdexcept>

//using etcdserverpb::Watch;
using etcdserverpb::WatchRequest;
using etcdserverpb::WatchResponse;
using etcdserverpb::WatchCreateRequest;
using grpc::Channel;

namespace etcd
{

  using etcdv3::watch_callback;

  class watch_error
      : public std::runtime_error
  {
  public:
//    using std::runtime_error::runtime_error;
//    using std::runtime_error::what;
    watch_error(etcd::StatusCode const etcd_error_code, std::string etcd_error_message)
      : _status(etcd_error_code, std::move(etcd_error_message))
    {}
    watch_error(grpc::StatusCode const grpc_error_code, grpc::string grpc_error_message)
      : _status(grpc_error_code, std::move(grpc_error_message))
    {}
    watch_error(etcd::Status status)
      : _status(std::move(status))
    {}

    const char * what() const noexcept override
    {
      try
      {
        return _status.grpc_error_message.empty() ? _status.etcd_error_message.c_str() :
            (_status.etcd_error_message + ": " + _status.grpc_error_message).c_str();
      }
      catch (...)
      {
        return _status.grpc_error_message.empty() ? _status.etcd_error_message.c_str() : _status.grpc_error_message.c_str();
      }
    }

    etcd::Status error_code() const noexcept
    {
      return !_status.grpc_is_ok() ? _status.grpc_error_code : _status.etcd_error_code;
    }

    etcd::Status status() const noexcept
    {
      return _status;
    }

  private:
    etcd::Status _status;
  };

//  class Watcher
//  {
//  public:
//    Watcher(
//        std::string const & address,
//        std::string const & key,
//        watch_callback callback,
//        pplx::task_options const & task_options = pplx::task_options());
//    Watcher(
//        std::shared_ptr<grpc::Channel> const & channel,
//        std::string const & key,
//        watch_callback callback,
//        pplx::task_options const & task_options = pplx::task_options());
//    Watcher(
//        std::string const & address,
//        std::string const & key,
//        bool const recursive,
//        int const fromRevision,
//        watch_callback callback,
//        pplx::task_options const & task_options = pplx::task_options());
//    Watcher(
//        std::shared_ptr<Channel> const & channel,
//        std::string const & key,
//        bool const recursive,
//        int const fromRevision,
//        watch_callback callback,
//        pplx::task_options const & task_options = pplx::task_options());
//    void cancel();
//    bool cancelled() const;
//    ~Watcher();

//  protected:
//    void doWatch();

//    const std::shared_ptr<Channel> channel;
//    const std::unique_ptr<Watch::Stub> watchServiceStub;
//    const pplx::task_options task_options;
//    etcdv3::ActionParameters watch_action_parameters;
//    watch_callback callback;
//    bool isCancelled;
//    pplx::task<void> currentTask;
//    std::unique_ptr<etcdv3::AsyncWatchAction> call;
//  };


  struct WatchActionParameters
  {
    WatchActionParameters() = default;
    WatchActionParameters(WatchActionParameters &&) = default;
    WatchActionParameters(WatchActionParameters const &) = default;
    int64_t revision = 0;
    bool recursive = false;
    bool prev_kv = false;
    bool progress_notify = false;
    std::string key;
    watch_callback callback;
  };


  using cq_tag_type = void *;

  // WatchClient must maintain shared_ptr to WatchKeeper(s)
  // class that keeps watches on single channel and with single completion queue
  // has its own cq, and reads it in separate thread
  // refreshes its watches, if needed
  // creates new and removes dead watches
  // if user tries to create watch from compacted revision, watch will be created from first available revision instead
  // watch will be created anyway, only server or connection problems won`t fire it immediately
  // user can check for errors through Watcher interface
  class WatchKeeper
      : public std::enable_shared_from_this<WatchKeeper>
  {
  public:
    WatchKeeper(
        std::shared_ptr<Channel> const & channel,
        pplx::task_options const & task_options = pplx::task_options())
      : _channel(channel)
      , _task_options(task_options)
      , _cq(new grpc::CompletionQueue())
      , _completion_queue_worker([this](){ completion_queue_worker_func(); }, _task_options)
    {}

    WatchKeeper(WatchKeeper const &) = delete;
    WatchKeeper & operator =(WatchKeeper const &) = delete;
    WatchKeeper(WatchKeeper &&) = delete;
    WatchKeeper & operator =(WatchKeeper &&) = delete;
    ~WatchKeeper()
    {
      _channel->NotifyOnStateChange(GRPC_CHANNEL_READY, std::chrono::system_clock::now(), _cq.get(), (void *)"shutdown");
      try
      {
        _completion_queue_worker.wait();
      }
      catch (...)
      {}

      std::lock_guard<std::mutex> lock(_watches_mutex);
      _watches.clear();
    }

    size_t size() const
    {
      std::lock_guard<std::mutex> lock(_watches_mutex);
      return _watches.size();
    }

    std::shared_ptr<Watcher> add_watch(
        std::string const & key,
        bool const recursive,
        int const from_revision,
        bool const prev_kv,
        bool const progress_notify,
        watch_callback const & callback)
    {
      WatchActionParameters parameters;
      parameters.key = key;
      parameters.recursive = recursive;
      parameters.revision = from_revision;
      parameters.prev_kv = prev_kv;
      parameters.progress_notify = progress_notify;
      parameters.callback = callback;

      std::lock_guard<std::mutex> lock(_watches_mutex);
      auto watch = std::make_shared<Watch>(_cq, etcdserverpb::Watch::NewStub(_channel), std::move(parameters));
      _watches[watch->tag()] = watch;
      return std::make_shared<Watcher>(watch);
    }

    void refresh()
    {
      _channel->NotifyOnStateChange(GRPC_CHANNEL_READY, std::chrono::system_clock::now(), _cq.get(), (void *)"refresh");
    }

  private:
    std::shared_ptr<Channel> const _channel;
    pplx::task_options const _task_options;
    std::shared_ptr<grpc::CompletionQueue> const _cq;
    mutable std::mutex _watches_mutex;
     std::map<cq_tag_type, std::shared_ptr<Watch>> _watches;
    pplx::task<void> _completion_queue_worker;

    void completion_queue_worker_func()
    {
      cq_tag_type got_tag;// = nullptr;
      bool ok = false;

      // never exit the loop until _cq->Shutdown() and _cq being fully drained
      // TODO: maybe wrap into try-catch to guarantee loop to be infinite
      while (_cq->Next(&got_tag, &ok))
      {
        if (got_tag == (void *)"shutdown")
        {
          std::lock_guard<std::mutex> lock(_watches_mutex);
          for (const auto & watch_pair : _watches)
          {
            const auto & watch = watch_pair.second;
            watch->on_cancel();
          }

          // at this point we guarantee that there would be no more _cq usage
          // because this means that we are in WatchKeeper`s destructor now
          _cq->Shutdown();

          continue;
        }

        if (got_tag == (void *)"refresh")
        {
          std::lock_guard<std::mutex> lock(_watches_mutex);
          for (const auto & watch_pair : _watches)
          {
            const auto & watch = watch_pair.second;
            watch->on_refresh();
          }

          continue;
        }

        // find watch corresponding to got_tag and process response
        // also check for requested to cancel and canceled (dead) watches
        std::lock_guard<std::mutex> lock(_watches_mutex);
        for (auto it = _watches.begin(); it != _watches.end();)
        {
          const auto & watch = it->second;
          if (watch->cancel_done())
          {
            it = _watches.erase(it);
          }
          else
          {
            if (watch->canceled())
            {
              watch->on_cancel();
            }
            else if (got_tag == watch->tag())
            {
              watch->on_response(ok);
            }
            ++it;
          }
        }
      }
    }

  };


  // handles gRPC calls, providing correct revision number, rpc re-creatring and canceling
  // must be used only within WatchKeeper due to its thread-usafety of state, rpc call and many more
  class Watch
  {
  public:

    enum class State
    {
      READY = 0,
      INIT = 1,
      CREATE_CALL = 2,
      SEND_CREATE_REQUEST = 3,
      READ_CREATE_RESPONSE = 4,
      CANCEL = 5,
      CANCELED = 6,
      FAILED = 7
    };

    Watch(
        std::shared_ptr<grpc::CompletionQueue> const & cq,
        std::unique_ptr<etcdserverpb::Watch::Stub> && watch_stub,
        WatchActionParameters && parameters)
      : _cq(cq)
      , _watch_stub(watch_stub)
      , _tag(this)
      , _parameters(std::move(parameters))
      , _state(State::INIT)
      , _cancel_requested(false)
      , _canceled(false)
      , _refresh_requested(false)
      , _last_error(nullptr)
    {
      create_call();
    }

    Watch(Watch const &) = delete;
    Watch & operator =(Watch const &) = delete;
    Watch(Watch &&) = delete;
    Watch & operator =(Watch &&) = delete;

    std::exception_ptr last_error() const
    {
      // TODO: is this mutex needed?
      std::lock_guard<std::mutex> lock(_last_error_mutex);
      return _last_error;
    }

    // return true when watch really canceled and rpc call is dead
    bool cancel_done() const
    {
      return _canceled.load();
    }

    // return true to user if cancel is requested
    bool canceled() const
    {
      return _cancel_requested.load();
    }

    // when cancel requested, no single callback will be called on watch events, but watch can be still alive some time
    void cancel()
    {
      _cancel_requested.store(true);
    }

    bool refreshing() const
    {
      return _refresh_requested.load();
    }

  protected:
    friend class WatchKeeper; // to access methods below from cq wroker

    // methods below MUST be called ONLY FROM cq worker!!!

    void on_response(bool const ok)
    {
      // if cancel requested and response received, do nothing
      // proceed only if state == CANCEL, what means that cancel process is finishing
      if (canceled() && state() != State::CANCEL)
      {
        return;
      }

      switch (state())
      {
        case State::INIT: return;
        case State::FAILED: return;
        case State::CANCELED: return;
        case State::CREATE_CALL:
          on_created(ok);
          break;
        case State::SEND_CREATE_REQUEST:
          on_sent_create_request(ok);
          break;
        case State::READ_CREATE_RESPONSE:
          on_read_create_response(ok);
          break;
        case State::READY:
          on_new_events(ok);
          break;
        case State::CANCEL:
          on_canceled(ok);
          break;
        default:
          break;
      }

      // after all done, if state became failed and refresh is requested, call on_refresh()
      if (refreshing() && state() == State::FAILED)
      {
        on_refreshed();
      }

    }

    void on_refresh()
    {
      // cannot refresh again if previous refreshing not finished
      if (refreshing())
      {
        return;
      }

      _refresh_requested.store(true);

      // failed or just initialized Watch has no interaction with cq, so we must call on_refresh immediately
      auto const current_state = state();
      if (current_state == State::FAILED || current_state == State::INIT)
      {
        on_refreshed();
      }
    }

    void on_cancel()
    {
      // if cancel has not been requested, do it
      if (!canceled())
      {
        cancel();
      }
      switch (state())
      {
        case State::FAILED:
          set_state(State::CANCELED);
          break;
        case State::CANCEL: return;
        case State::CANCELED: return;
        default:
        {
          set_state(State::CANCEL);
          _rpc->WritesDone(_tag);
          break;
        }
      }
    }

    cq_tag_type const tag() const
    {
      return _tag;
    }

  private:
    std::shared_ptr<grpc::CompletionQueue> const _cq;
    std::unique_ptr<etcdserverpb::Watch::Stub> const _watch_stub;
    cq_tag_type const _tag;
    WatchActionParameters _parameters;
    // TODO: if Prepare needed only once, make this const and don`t store _watch_stub and _cq as class members
    std::unique_ptr<ClientAsyncReaderWriter<WatchRequest, WatchResponse>> _rpc;
    State _state;
    std::atomic_bool _cancel_requested;
    std::atomic_bool _canceled;
    std::atomic_bool _refresh_requested;
    grpc::ClientContext _context;
    grpc::Status _status;
    WatchResponse _response;
    mutable std::mutex _last_error_mutex;
     std::exception_ptr _last_error;

    void set_state(State const state)
    {
      _state = state;
    }

    State state() const
    {
      return _state;
    }

    void create_call()
    {
      set_state(State::CREATE_CALL);
      clear_error();
      _rpc.reset(_watch_stub->PrepareAsyncWatch(&_context, _cq.get()));
      _rpc->startCall(_tag);
    }

    void on_created(bool const ok)
    {
      if (!ok)
      {
        set_state(State::FAILED);
        store_error(etcd::Status(etcd::StatusCode::UNDERLYING_GRPC_ERROR, "Start call gRPC method for watch failed"));
        return;
      }

      auto from_revision_explicit = _parameters.revision;
      if (from_revision_explicit == 0)
      {
        const auto stub = etcdserverpb::KV::NewStub(_watch_keeper->_channel);
        etcdv3::ActionParameters get_params;
        get_params.key.assign(_parameters.key);
        get_params.withPrefix = _parameters.recursive;
        get_params.kv_stub = stub.get();
        auto get_call = std::make_shared<etcdv3::AsyncGetAction>(std::move(get_params));
        get_call->waitForResponse();
        from_revision_explicit = get_call->ParseResponse().revision;
        if (from_revision_explicit == 0)
        {
          set_state(State::FAILED);
          store_error(etcd::Status(
                        etcd::StatusCode::UNDERLYING_GRPC_ERROR,
                        "Getting the current revision of the etcd key-value store failed"));
          return;
        }
      }

      // if fromRevision is 0, interested revision for user is current + 1
      _parameters.revision = from_revision_explicit + (_parameters.revision == 0 ? 1 : 0);

      WatchRequest watch_req;
      {
        WatchCreateRequest watch_create_req;
        watch_create_req.set_key(_parameters.key);
        watch_create_req.set_prev_kv(_parameters.prev_kv);
        watch_create_req.set_start_revision(_parameters.revision);
        watch_create_req.set_progress_notify(_parameters.progress_notify);

        if(_parameters.recursive)
        {
          std::string range_end(_parameters.key);
          int ascii = static_cast<int>(range_end[range_end.length() - 1]);
          range_end.back() = ascii + 1;
          watch_create_req.set_range_end(std::move(range_end));
        }

        *(watch_req.mutable_create_request()) = std::move(watch_create_req);
      }

      set_state(State::SEND_CREATE_REQUEST);

      _rpc->Write(watch_req, _tag);
    }

    void on_sent_create_request(bool const ok)
    {
      if (!ok)
      {
        set_state(State::FAILED);
        store_error(etcd::Status(etcd::StatusCode::UNDERLYING_GRPC_ERROR, "Sending create request for watch failed"));
        return;
      }

      set_state(State::READ_CREATE_RESPONSE);
      _rpc->Read(&_response, _tag);
    }

    void on_read_create_response(bool const ok)
    {
      if (!ok)
      {
        set_state(State::FAILED);
        store_error(etcd::Status(
                      etcd::StatusCode::UNDERLYING_GRPC_ERROR,
                      "Reading create response for watch from etcd server failed"));
        return;
      }

      if (_response.compact_revision() > 0)
      {
        store_error(etcd::Status(
                      etcd::StatusCode::WATCH_COMPACTED_REVISION,
                      "Specified start revision for watch is compacted"));
        _parameters.revision = _response.compact_revision();
        on_created(true);
        return;
      }

      if (_response.canceled())
      {
        set_state(State::FAILED);
        store_error(etcd::Status(
                      etcd::StatusCode::WATCH_CANCELED_BY_SERVER,
                      "Watch has been canceled by etcd server, reason: " + _response.cancel_reason()));
        return;
      }

      if (_response.created())
      {
        set_state(State::READY);
        clear_error();
        _rpc->Read(&_response, _tag);
        return;
      }

      set_state(State::FAILED);
      store_error(etcd::Status(etcd::StatusCode::UNKNOWN_ERROR, "Watch has not been created for unknown reason"));
      return;
    }

    void on_new_events(bool const ok)
    {
      if (!ok)
      {
        set_state(State::FAILED);
        store_error(etcd::Status(
                      etcd::StatusCode::UNDERLYING_GRPC_ERROR,
                      "Watch failed due to underlying gRPC error, most likely it is a connection problem"));
        return;
      }

      // update last detected revision with watched key change (only if revision is presented)
      _parameters.revision = _response.header().revision() > 0 ? _response.header().revision() : _parameters.revision;

      // if ok and read done, call callback (only if reply has events) and just read again
      if (_response.events_size() > 0)
      {
        // TODO: do it asynchronously
        // callback is responsible for errors handling, all raised exceptions will be ignored
        try
        {
          _parameters.callback(etcd::Response(parse_response()));
        }
        catch (...)
        {}
      }

      // set_state(State::READY);
      // clear_error();
      _rpc->Read(&_response, _tag);
    }

    void on_refreshed()
    {
      _refresh_requested.store(false);

      // no effect on canceled watch
      if (canceled())
      {
        return;
      }

      if (_rpc && _parameters.revision > 0)
      {
        _parameters.revision++;
      }
      create_call();
    }

    void on_canceled(bool const ok)
    {
      if (!ok)
      {
        set_state(State::FAILED);
        store_error(etcd::Status(
                      etcd::StatusCode::WATCH_CANCELATION_ERROR,
                      "Watch cancelation failed, although watch has been canceled anyway"));
      }
      set_state(State::CANCELED);
      _canceled.store(true);
    }

    void store_error(etcd::Status status)
    {
      std::lock_guard<std::mutex> lock(_last_error_mutex);
      _last_error = std::make_exception_ptr(watch_error(std::move(status)));
    }

    void clear_error()
    {
      std::lock_guard<std::mutex> lock(_last_error_mutex);
      _last_error = nullptr;
    }

    etcdv3::AsyncWatchResponse parse_response()
    {
      if (!_status.ok())
      {
        return etcdv3::AsyncWatchResponse(_status.error_code(), _status.error_message());
      }

      return etcdv3::AsyncWatchResponse(_response); // TODO: maybe move reponse away from this?
    }

  };


  class Watcher
  {
  public:
    Watcher(std::shared_ptr<Watch> watch)
      : _weak_watch(std::move(watch))
    {}
    Watcher(Watcher const &) = delete;
    Watcher & operator =(Watcher const &) = delete;
    Watcher(Watcher &&) = delete;
    Watcher & operator =(Watcher &&) = delete;

    void cancel()
    {
      std::lock_guard<std::mutex> lock(_weak_watch_mutex);
      auto const watch = _weak_watch.lock();
      if (watch)
      {
        watch->cancel();
      }
    }

    bool canceled() const
    {
      std::lock_guard<std::mutex> lock(_weak_watch_mutex);
      auto const watch = _weak_watch.lock();
      if (!watch)
      {
        return true;
      }
      return watch->canceled();
    }

    etcd::Status last_error() const
    {
      std::exception_ptr exc = nullptr;
      {
        std::lock_guard<std::mutex> lock(_weak_watch_mutex);
        auto const watch = _weak_watch.lock();
        if (!watch || watch->canceled())
        {
          return etcd::Status(etcd::StatusCode::WATCH_CANCELED_BY_USER, "Watch has been canceled");
        }
        exc = watch->last_error();
      }

      if (exc)
      {
        try
        {
          std::rethrow_exception(exc);
        }
        catch (watch_error const & e)
        {
          return e.status();
        }
        catch (std::exception const & e)
        {
          return etcd::Status(etcd::StatusCode::UNKNOWN_ERROR, e.what());
        }
      }
      return etcd::Status(etcd::StatusCode::OK, "");
    }

  private:
      mutable std::mutex _weak_watch_mutex;
       std::weak_ptr<Watch> _weak_watch;
  };


} // namespace etcd

#endif
