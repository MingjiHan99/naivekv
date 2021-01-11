//
// Created by Han Mingji on 2020/12/30.
//

#include <istream>
#include <fstream>
#include <iostream>
#include "sstable.h"

void SSTable::init() {

    file_ = new std::fstream(file_name_, std::ios::in | std::ios::binary);

    uint64_t min_key_length;
    uint64_t max_key_length;

    file_->read(reinterpret_cast<char *>(&min_key_length), sizeof(uint64_t));
    char min_key_chr[min_key_length];
    file_->read(min_key_chr, min_key_length);
    min_key_ = std::string(min_key_chr);

    file_->read(reinterpret_cast<char *>(&max_key_length), sizeof(uint64_t));
    char max_key_chr[max_key_length];
    file_->read(max_key_chr, max_key_length);
    max_key_ = std::string(max_key_chr);

    uint64_t k, length;
    file_->read(reinterpret_cast<char *>(&k), sizeof(uint64_t));
    file_->read(reinterpret_cast<char *>(&length), sizeof(uint64_t));
    int array[length];
    file_->read(reinterpret_cast<char *>(array), length * sizeof(int));

    bf_ = new BloomFilter(k ,length);
    bf_->set_bit_vec(array);

}

std::string SSTable::get_min_key() {
    return min_key_;
}

std::string SSTable::get_max_key() {
    return max_key_;
}



std::tuple<std::string, bool, uint64_t> SSTable::get(std::string key) {
    if (bf_->check_key(key)) {
        if (!loaded) {
            load_table();
        }
        auto iter = kv_.find(key);
        if (iter != kv_.end()) {
            return make_tuple(iter->second.first, true, iter->second.second);
        }
    }

    return make_tuple(std::string(""), false, 0);
}

void SSTable::load_table() {

    uint64_t time_stamp, key_len, val_len;
    while(file_->read(reinterpret_cast<char*>(&time_stamp), sizeof(uint64_t))) {
        file_->read(reinterpret_cast<char*>(&key_len), sizeof(uint64_t));
        file_->read(reinterpret_cast<char*>(&val_len), sizeof(uint64_t));
        //  printf("Key length: %llu Value length: %llu\n", key_len, val_len);
        char key[key_len];
        char val[val_len];
        file_->read(reinterpret_cast<char*>(key), key_len);
        file_->read(reinterpret_cast<char*>(val), val_len);
        kv_[std::string(key)] = make_pair(std::string(val), time_stamp);
    }

    loaded = true;
}

const std::unordered_map<std::string, std::pair<std::string, uint64_t>>& SSTable::get_kv() {
    if (!loaded) {
        load_table();
    }
    return kv_;
}