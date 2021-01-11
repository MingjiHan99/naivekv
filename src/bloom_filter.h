//
// Created by Han Mingji on 2020/12/27.
//

#ifndef LSM_KV_BLOOM_FILTER_H
#define LSM_KV_BLOOM_FILTER_H

#include <string>

class BloomFilter {
public:
    BloomFilter(uint64_t k, uint64_t length) {
        k_ = k;
        length_ = length;
        bit_vec_ = new int[length];

    }

    ~BloomFilter() {
        delete []bit_vec_;
    }

    void add_key(std::string key);
    void set_bit_vec(int *bit_vec);
    int *get_bit_vec();
    uint64_t get_length();
    bool check_key(std::string key);

    uint64_t get_k();

private:
    uint64_t k_;
    uint64_t length_;
    int *bit_vec_;

};

#endif //LSM_KV_BLOOM_FILTER_H
