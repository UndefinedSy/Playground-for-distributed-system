#ifndef _CONSISTENT_HASH_H_
#define _CONSISTENT_HASH_H_

#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

class consistent_hash
{
private:
    // A circle stores the hash value and corresponding node_address
    std::map<uint32_t, std::string>         circle_;
    // Record whether the nodes are still live
    std::map<std::string, bool>             members_;
    // A sorted list of the items on the ring
    // can be regard as a sorted center angle(hmmm... you know what I mean)
    std::vector<uint32_t>                   sorted_ring_;
    int                                     nodes_num_;
    // the num of the virtual nodes
    int                                     virtual_nodes_num_;
    std::mutex                              rw_mutex_;
    std::function<uint32_t(std::string)>    hash_method_;

    void update_sorted_ring();
    int search_node(uint32_t hashed_key);

public:
    consistent_hash(int virtual_nodes_num, std::function<uint32_t(std::string)> hash_method);
    ~consistent_hash();

    // add a new node into the hash ring
    void add_node(std::string node_addr);
    // remove a node from the hash ring
    void remove_node(std::string node_addr);
    // find which server can handle a request with key
    std::string find_node(std::string key);
};

#endif