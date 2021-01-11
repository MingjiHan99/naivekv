//
// Created by Han Mingji on 2020/12/27.
//

#ifndef LSM_KV_SSTABLE_H
#define LSM_KV_SSTABLE_H



#include <string>
#include <unordered_map>
#include "bloom_filter.h"

class SSTable {
public:
    SSTable(std::string file_name) {
        file_name_ = file_name;
        bf_ = nullptr;
        loaded = false;
        init();
    }

    ~SSTable() {
        delete bf_;
        file_->close();
        delete file_;
    }

    std::tuple<std::string, bool, uint64_t> get(std::string key);

    std::string get_min_key();

    std::string get_max_key();

    const std::unordered_map<std::string, std::pair<std::string, uint64_t>> &get_kv();

private:
    bool loaded;
    BloomFilter *bf_;
    std::string file_name_;
    std::fstream *file_;
    std::unordered_map<std::string, std::pair<std::string, uint64_t>> kv_;
    std::string min_key_;
    std::string max_key_;

    void init();

    void load_table();

};
#endif //LSM_KV_SSTABLE_H
