//
// Created by Han Mingji on 2021/1/7.
//

#ifndef LSM_KV_COMPACTOR_H
#define LSM_KV_COMPACTOR_H

#include <cstdint>
#include "file_wrapper.h"
#include "file_version.h"
#include "sstable.h"
#include <queue>
#include "mem_table.h"

struct SortItem {
    std::string key_;
    std::string value_;
    uint64_t time_stamp_;
    uint64_t level_;

    SortItem(std::string key, std::string value, uint64_t time_stamp, uint64_t level)
        :key_{key},
         value_{value},
         time_stamp_{time_stamp},
         level_{level} {

    }

    friend std::ostream& operator << (std::ostream& os, const SortItem& item) {
        os << "key: " << item.key_ << " level: " << item.level_ << " time_stamp:" << item.time_stamp_ << std::endl;
    }


    bool operator < (const SortItem& rhs) const {
        return key_ > rhs.key_ || (key_ == rhs.key_) && (level_ < rhs.level_) ||
        (key_ == rhs.key_)  && (level_ == rhs.level_) && (time_stamp_ > rhs.time_stamp_);
    }
};

class Compactor {
public:
    Compactor(uint64_t level):
        level_{level}{
        last_merged_keys_ = new std::string[level_];
    }


    ~Compactor() {
        delete []last_merged_keys_;
    }
    std::pair<MemTable*, std::vector<std::string>> l0_compact(FileVersion* file_version);
    MemTable* merge_sort(const std::vector<std::string>& file_names);
    static std::vector<std::string> find_intersect_files(std::string min_key, std::string max_key, int level, FileVersion* file_version);
private:
    uint64_t level_;
    std::string *last_merged_keys_;

};


#endif //LSM_KV_COMPACTOR_H
