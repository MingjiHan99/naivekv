#include <zconf.h>
#include "naive_db.h"
#include "sstable.h"
#define SCALE 10000
/*
int main() {

    NaiveDB naive_db("/Users/Han Mingji/CLionProjects/lsm-kv/naivedb/");
    for (int i = 1; i < SCALE; i++) {
        std::cout << i << std::endl;
        naive_db.put(std::to_string(i), std::to_string(i));
    }


    for (int i = 1; i < SCALE; i++) {
        naive_db.del(std::to_string(i));
        std::cout << "DEL " << i << std::endl;
        assert(naive_db.get(std::to_string(i)) == "");
    }

    return 0;
}*/