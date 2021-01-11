//
// Created by Han Mingji on 2020/12/20.
//
#include <cstring>
#include "mem_table.h"
#include "log_item.h"
#include "bloom_filter.h"
#include "naive_db.h"
ListNode *MemTable::get(std::string key) {
    ListNode *node = list[MAX_LEVEL - 1]->head;
    for (int i = MAX_LEVEL - 1; i >= 0; i--) {
        if (node->next_[i] == list[i]->tail && i != 0) {
            node = list[i-1]->head;
        } else {

            if (i != MAX_LEVEL - 1 && node == list[i+1]->head) {
                node = list[i]->head;
            }
           while (node->next_[i] != list[i]->tail && key > node->next_[i]->key_) {
                node = node->next_[i];
            }
            if (node->next_[i] != list[i]->tail && node->next_[i]->key_ == key) {

                return node->next_[i];
            }

        }

    }
    return nullptr;
}



uint64_t MemTable::random_level() {
    uint64_t level = 0;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, 1);
    auto p = dis(gen);
    while (level < MAX_LEVEL - 1 && p < P) {
        level++;
        p = dis(gen);
    }
    return level;
}



void MemTable::put(uint64_t time_stamp, std::string key, std::string value) {
    auto p = get(key);
    if (!p) {
        memory_usage_ += key.size() + value.size() + 8;
        uint64_t level = random_level();
        ListNode *node = new ListNode(time_stamp, key, value);

        for (uint64_t i = 0; i <= level; i++) {
            list[i]->insert_node(node);
        }
    } else {
        p->value_ = value;
    }
}

void MemTable::display_all_list() {
    int i = MAX_LEVEL - 1;
    while (i >= 0) {
        std::cout << "Level " << i << " :";
        list[i]->print_debug();
        i--;
    }
    std::cout << "Done" << std::endl;
}



void MemTable::del_ref() {
    ref_--;
    assert(ref_>=0);
    if (ref_ <= 0) {
        delete this;
    }
}

void MemTable::add_ref() {
    ref_++;
}

void MemTable::dump_to_file(std::ostream& os) {
    BloomFilter bf{K, BIT_LENGTH};

    auto p = list[0]->head->next_[0], q = list[0]->head->next_[0];
    std::string min_key = p->key_;
    std::string max_key;
    while (q != list[0]->tail) {
        bf.add_key(q->key_);
        max_key = q->key_;
        q = q->next_[0];
    }

    uint64_t min_key_length = min_key.length() + 1;
    uint64_t max_key_length = max_key.length() + 1;
    uint64_t k = bf.get_k();
    uint64_t length = bf.get_length();

    os.write(reinterpret_cast<char *>(&min_key_length), sizeof(uint64_t));
    os.write(min_key.c_str(), min_key_length);
    os.write(reinterpret_cast<char *>(&max_key_length), sizeof(uint64_t));
    os.write(max_key.c_str(), max_key_length);
    os.write(reinterpret_cast<char *>(&k), sizeof(uint64_t));
    os.write(reinterpret_cast<char *>(&length), sizeof(uint64_t));
    os.write(reinterpret_cast<char *>(bf.get_bit_vec()), length * sizeof(int));

    while (p != list[0]->tail) {

        uint64_t key_len = p->key_.length() + 1;
        uint64_t val_len = p->value_.length() + 1;
        char arr[3 * sizeof(uint64_t) + key_len + val_len];

        memcpy(arr, &p->time_stamp_, sizeof(uint64_t));
        memcpy(arr + sizeof(uint64_t), &key_len,  sizeof(uint64_t));
        memcpy(arr + 2 * sizeof(uint64_t), &val_len , sizeof(uint64_t));
        memcpy(arr + 3 * sizeof(uint64_t), p->key_.c_str(), key_len);
        memcpy(arr + 3 * sizeof(uint64_t) + key_len, p->value_.c_str(), val_len);
        os.write(arr, sizeof(arr));


        p = p->next_[0];
    }
    std::cout << "Dump done" << std::endl;
}


