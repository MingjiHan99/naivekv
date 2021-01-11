
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Suites
#include <boost/test/unit_test.hpp>

#include "../src/bloom_filter.h"
#include "../src/naive_db.h"
#include <iostream>
#include "../src/list.h"
#include "../src/naive_db.h"
#include <iostream>

#define PATH "/mnt/d/Code/lsm-kv/naive_db/"
const uint64_t DATA_SIZE = 5000;
BOOST_AUTO_TEST_SUITE(list)

    BOOST_AUTO_TEST_CASE(test_compaction) {
        NaiveDB naive_db(PATH);
        for (int i = 0;i  < DATA_SIZE; i++) {
            naive_db.put(std::to_string(i), std::to_string(i + 100));
        }

        for (int i = 0;i  < DATA_SIZE; i++) {
            naive_db.put(std::to_string(i), std::to_string(i + 200));
        }

        for (int i = 0;i  < DATA_SIZE; i++) {
            std::cout << "reading " << i << " "<< naive_db.get(std::to_string(i)) << std::endl;
         //   BOOST_ASSERT(naive_db.get(std::to_string(i)) == std::to_string(i + 200));
        }
    };



BOOST_AUTO_TEST_SUITE_END()//
// Created by Han Mingji on 2020/12/27.
//

