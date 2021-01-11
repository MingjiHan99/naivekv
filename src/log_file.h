//
// Created by Han Mingji on 2020/12/18.
//

#ifndef LSM_KV_LOG_FILE_H
#define LSM_KV_LOG_FILE_H

#include "log_item.h"
#include <string>
#include <fstream>
#include <iostream>
#include <utility>


class LogFile {
public:
    LogFile(std::string file_name) {
        file_name_ = std::move(file_name);

        std::ifstream f(file_name_);
        if (!f.good()) {
            std::ofstream new_file(file_name_);
        }
        file_ = new std::fstream(file_name_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }

    ~LogFile() {
        file_->flush();
        file_->close();
        delete file_;
    }

    void write_log(uint64_t time_stamp, Operation op, std::string key, std::string value);
    std::vector<LogItem> parse_log();

private:

    std::string file_name_;
    std::fstream *file_ = nullptr;
};


#endif //LSM_KV_LOG_FILE_H
