//
// Created by Han Mingji on 2020/12/27.
//

#include <cstring>
#include "bloom_filter.h"
#include "murmurhash.h"

bool BloomFilter::check_key(std::string key) {
    uint32_t value;
    for (int i = 0; i < k_; i++) {
        MurmurHash3_x86_32(key.c_str(), i, key.length(), &value);
        if (!bit_vec_[value % length_]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::add_key(std::string key) {
    uint32_t value;
    for (int i = 0; i < k_; i++) {
        MurmurHash3_x86_32(key.c_str(), i, key.length(), &value);
        bit_vec_[value % length_] = 1;
    }
}

void BloomFilter::set_bit_vec(int *bit_vec) {
    memcpy(bit_vec_, bit_vec, sizeof(int) * length_);
}

uint64_t BloomFilter::get_k() {
    return k_;
}

uint64_t BloomFilter::get_length() {
    return length_;
}

int *BloomFilter::get_bit_vec() {
    return bit_vec_;
}

