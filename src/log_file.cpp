//
// Created by Han Mingji on 2020/12/18.
//

#include <vector>
#include "log_file.h"
#include <experimental/filesystem>
#include <cstring>


void LogFile::write_log( std::uint64_t time_stamp, Operation op, std::string key, std::string value) {
    //append write
    // 8 (time_stamp) + 8 (op) + 8 (key_len) + 8 (val_len) + key + '\0' + value + '\0'
    uint64_t  key_len = key.length() + 1;
    uint64_t  val_len = value.length() + 1;
    char arr[4 * sizeof(uint64_t) + key_len + val_len];
    uint64_t op_val = 0;
    if (op == Operation::DELETE) {
        op_val = 1;
    }
    memcpy(arr, &time_stamp, sizeof(uint64_t));
    memcpy(arr + sizeof(uint64_t), &op_val,  sizeof(uint64_t));
    memcpy(arr + 2 * sizeof(uint64_t),&key_len , sizeof(uint64_t));
    memcpy(arr + 3 * sizeof(uint64_t), &val_len, sizeof(uint64_t));
    memcpy(arr + 4 * sizeof(uint64_t), const_cast<char*>(key.c_str()),  key_len);
    memcpy(arr + 4 * sizeof(uint64_t) + key_len, const_cast<char*>(value.c_str()), val_len);
    file_->write(arr, sizeof(arr));

}

std::vector<LogItem> LogFile::parse_log() {
    std::vector<LogItem> items;
    uint64_t time_stamp = 0;
    uint64_t op = 0;
    uint64_t key_length = 0;
    uint64_t val_length = 0;
    file_->seekg(0, std::ios::beg);
    while (file_->read(reinterpret_cast<char*>(&time_stamp), sizeof(uint64_t))) {
        file_->read(reinterpret_cast<char*>(&op), sizeof(uint64_t));
        file_->read(reinterpret_cast<char*>(&key_length), sizeof(uint64_t));
        file_->read(reinterpret_cast<char*>(&val_length), sizeof(uint64_t));
        char key[key_length];
        file_->read(key, key_length);
        char val[val_length];
        file_->read(val, val_length);
        items.emplace_back(time_stamp, op == 0 ? Operation::PUT : Operation::DELETE, std::string(key), std::string(val));
    }
    file_->clear(); // important
    return items;
}