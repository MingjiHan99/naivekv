//
// Created by Han Mingji on 2020/12/20.
//

#ifndef LSM_KV_LIST_H
#define LSM_KV_LIST_H

#include <iostream>
#include "list_node.h"

struct List {
    List(uint64_t level) {
        level_ = level;
        head = new ListNode();
        tail = new ListNode();
        head->next_[level_] = tail;
    }

    ~List() {
        delete head;
        delete tail;
    }

    void insert_node(ListNode *node);
    void delete_node( std::string key);
    ListNode* find_node(std::string key);
    void print_debug();

    uint64_t level_;
    ListNode *head;
    ListNode *tail;
};




#endif //LSM_KV_LIST_H
