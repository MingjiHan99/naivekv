//
// Created by Han Mingji on 2021/1/6.
//

#include "file_wrapper.h"

void FileWrapper::init() {
    std::fstream file{file_name_, std::ios::binary | std::ios::in};
    uint64_t min_key_length;
    uint64_t max_key_length;

    file.read(reinterpret_cast<char *>(&min_key_length), sizeof(uint64_t));
    char min_key_chr[min_key_length];
    file.read(min_key_chr, min_key_length);
    min_key_ = std::string(min_key_chr);

    file.read(reinterpret_cast<char *>(&max_key_length), sizeof(uint64_t));
    char max_key_chr[max_key_length];
    file.read(max_key_chr, max_key_length);
    max_key_ = std::string(max_key_chr);

}

std::string FileWrapper::get_max_key() const {
    return max_key_;
}

std::string FileWrapper::get_min_key() const {
    return min_key_;
}

std::string FileWrapper::get_file_name() const {
    return file_name_;
}
