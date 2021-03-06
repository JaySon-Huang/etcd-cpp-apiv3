#ifndef __ETCD_SYNC_CLIENT_HPP__
#define __ETCD_SYNC_CLIENT_HPP__

#include <etcd/Client.hpp>

namespace etcd
{
  /**
   * SyncClient is a wrapper around etcd::Client and provides a simplified sync interface with blocking operations.
   *
   * In case of any exceptions occur in the backend the Response value's error_message will contain the
   * text of the exception and the error code will be 600
   *
   * Use this class only if performance does not matter.
   */
  class SyncClient
  {
  public:
    /**
     * Constructs an etcd sync client object.
     * @param etcd_url is the url of the etcd server to connect to, like "http://127.0.0.1:4001"
     */
    SyncClient(std::string const & etcd_url);

    Response get(std::string const & key);
    Response set(std::string const & key, std::string const & value, int const ttl = 0);
    Response set(std::string const & key, std::string const & value, int64_t const lease_id);
    Response add(std::string const & key, std::string const & value, int const ttl = 0);
    Response add(std::string const & key, std::string const & value, int64_t const lease_id);
    Response modify(std::string const & key, std::string const & value, int const ttl = 0);
    Response modify(std::string const & key, std::string const & value, int64_t const lease_id);
    Response modify_if(std::string const & key, std::string const & value, std::string const & old_value, int const ttl = 0);
    Response modify_if(std::string const & key, std::string const & value, std::string const & old_value, int64_t const lease_id);
    Response modify_if(std::string const & key, std::string const & value, int64_t const old_revision, int const ttl = 0);
    Response modify_if(std::string const & key, std::string const & value, int64_t const old_revision, int64_t const lease_id);
    Response rm(std::string const & key);
    Response rm_if(std::string const & key, std::string const & old_value);
    Response rm_if(std::string const & key, int64_t const old_revision);
    Response ls(std::string const & key);
    Response mkdir(std::string const & key, int const ttl = 0);
    Response rmdir(std::string const & key, bool const recursive = false);
    Response leasegrant(int const ttl);

    /**
     * Watches for changes of a key or a subtree. Please note that if you watch e.g. "/testdir" and
     * a new key is created, like "/testdir/newkey" then no change happened in the value of
     * "/testdir" so your watch will not detect this. If you want to detect addition and deletion of
     * directory entries then you have to do a recursive watch.
     * @param key is the value or directory to be watched
     * @param recursive if true watch a whole subtree
     */
    // Response watch(std::string const & key, bool const recursive = false);

    /**
     * Watches for changes of a key or a subtree from a specific index. The index value can be in the "past".
     * @param key is the value or directory to be watched
     * @param fromIndex the first index we are interested in
     * @param recursive if true watch a whole subtree
     */
    // Response watch(std::string const & key, int const from_revision, bool const recursive = false);


  protected:
    Client client;
  };
}

#endif
