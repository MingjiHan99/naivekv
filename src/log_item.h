//
// Created by Han Mingji on 2020/12/18.
//

#ifndef LSM_KV_LOG_ITEM_H
#define LSM_KV_LOG_ITEM_H

#include <cstdint>
#include <string>

enum Operation {
    PUT = 0,
    DELETE
};
struct LogItem {
    Operation op_;
    std::string key_;
    std::string value_;
    uint64_t time_stamp_;
    LogItem(uint64_t time_stamp, Operation op, std::string key, std::string value) {
        time_stamp_ = time_stamp;
        op_ = op;
        key_ = key;
        value_ = value;
    }
};


#endif //LSM_KV_LOG_ITEM_H
