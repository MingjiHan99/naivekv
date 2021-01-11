
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Suites
#include <boost/test/unit_test.hpp>
#include "../src/list.h"
#include "../src/naive_db.h"
#include <iostream>

#define PATH "/mnt/d/Code/lsm-kv/naive_db/"
BOOST_AUTO_TEST_CASE(test_insert) {
    NaiveDB naive_db(PATH);
    for (int i = 0;i  < 5000; i++) {
        naive_db.put(std::to_string(i), std::to_string(i + 100));
    }

};

BOOST_AUTO_TEST_CASE(test_query) {
    NaiveDB naive_db(PATH);

        for (int i = 0;i  < 5000; i++) {
            auto res = naive_db.get(std::to_string(i));
            std::cout << "reading " << i << " " << res  << std::endl;
            BOOST_ASSERT(naive_db.get(std::to_string(i)) == std::to_string(i + 100));
        }
}


BOOST_AUTO_TEST_CASE(test_update) {
    NaiveDB naive_db(PATH);

    for (int i = 0;i  < 5000; i++) {
        naive_db.put(std::to_string(i), std::to_string(i + 200));

    }
}

BOOST_AUTO_TEST_CASE(test_query2) {
    NaiveDB naive_db(PATH);

    for (int i = 0;i  < 5000; i++) {
        std::cout << "reading " << i << " "<< naive_db.get(std::to_string(i)) << std::endl;
        BOOST_ASSERT(naive_db.get(std::to_string(i)) == std::to_string(i + 200));
    }
}
