//
// Created by Han Mingji on 2020/12/24.
//

#ifndef LSM_KV_META_DATA_H
#define LSM_KV_META_DATA_H

#include <string>

struct MetaData {
    MetaData() {
        ref_ = 1;
    }

    ~MetaData() {

    }

    void add_ref() {
        ref_++;
    }
    void del_ref() {
        ref_--;
        if (ref_ <= 0) {
            delete this;
        }
    }
    uint64_t next_log_number_; // next log number, log_number_ - 1 is the current log number (if log_number_ > 0)
    bool is_imm_table_;
    uint64_t l0_sst_start_;
    uint64_t l0_sst_next_;
    uint64_t l1_sst_start_;
    uint64_t l1_sst_end_;
    uint64_t ref_;
};

#endif //LSM_KV_META_DATA_H
