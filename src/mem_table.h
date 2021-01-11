//
// Created by Han Mingji on 2020/12/20.
//

#ifndef LSM_KV_MEM_TABLE_H
#define LSM_KV_MEM_TABLE_H
#include <cstdint>
#include <random>
#include <iostream>
#include "list.h"

const double P = 0.5;

class MemTable {
public:
    MemTable() {
        for (uint64_t i = 0; i < MAX_LEVEL; i++) {
            list[i] = new List{i};
        }
        memory_usage_ = 0;
        ref_ = 1;
    }

    ~MemTable() {

        ListNode *nxt = nullptr, *p = list[0]->head->next_[0];
        while (p != list[0]->tail) {
            nxt = p->next_[0];
            delete p;
            p = nxt;
        }

        for (uint64_t i = 0;i < MAX_LEVEL; i++) {
            delete list[i];
        }

    }

    void put(uint64_t time_stamp, std::string key, std::string value);

    ListNode* get(std::string key);
    uint64_t get_memory_usage() {
        return memory_usage_;
    }
    void remove(std::string key);

    void display_all_list();

    void add_ref();

    void del_ref();

    void dump_to_file(std::ostream& os);

private:
    uint64_t memory_usage_;
    uint64_t ref_;
    uint64_t random_level();
    List *list[MAX_LEVEL];
};



#endif //LSM_KV_MEM_TABLE_H
