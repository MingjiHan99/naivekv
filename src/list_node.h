//
// Created by Han Mingji on 2020/12/20.
//

#ifndef LSM_KV_LIST_NODE_H
#define LSM_KV_LIST_NODE_H
#define MAX_LEVEL 8
#include <string>

struct ListNode {
    uint64_t time_stamp_;
    std::string key_;
    std::string value_;
    ListNode *next_[MAX_LEVEL];
    ListNode() {
        init_next();
    }
    ListNode(uint64_t time_stamp, std::string key,  std::string value) : time_stamp_(time_stamp), key_(key), value_(value) {
        init_next();
    }

    void init_next() {
        for (int i = 0; i < MAX_LEVEL; i++) {
            next_[i] = nullptr;
        }
    }
};

#endif //LSM_KV_LIST_NODE_H
