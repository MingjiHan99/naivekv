
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Suites
#include <boost/test/unit_test.hpp>

#include "../src/bloom_filter.h"
#include <iostream>
BOOST_AUTO_TEST_SUITE(list)

    BOOST_AUTO_TEST_CASE(test_bloom) {
        BloomFilter bloom{10, 10000};
        std::vector<std::string> keys{"alpha", "beta", "gammma", "mysql"};
        for (auto& key:keys) {
            bloom.add_key(key);
        }

        for (auto& key:keys) {
            BOOST_ASSERT(bloom.check_key(key));
        }
    };



BOOST_AUTO_TEST_SUITE_END()//
// Created by Han Mingji on 2020/12/27.
//

