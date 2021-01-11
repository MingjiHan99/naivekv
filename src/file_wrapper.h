//
// Created by Han Mingji on 2021/1/6.
//

#ifndef LSM_KV_FILE_WRAPPER_H
#define LSM_KV_FILE_WRAPPER_H

#include <string>
#include <fstream>
#include <experimental/filesystem>
#include <iostream>

namespace fs = std::experimental::filesystem;
class FileWrapper {
public:
    FileWrapper(std::string file_name) {
        ref_ = 1;
        file_name_ = file_name;
        init();
    }
    void add_ref() {
        ref_++;
    }

    void dec_ref(bool remove_file) {
        ref_--;
        if (ref_ <= 0) {
            if (remove_file) {
                std::cout << "Remove file: " << file_name_ << std::endl;
                fs::remove(file_name_);
            }
            delete this;
        }
    }

    std::string get_max_key() const;

    std::string get_min_key() const;

    std::string get_file_name() const;
private:
    int ref_;
    std::string max_key_;
    std::string min_key_;
    std::string file_name_;
    void init();

};


#endif //LSM_KV_FILE_WRAPPER_H


