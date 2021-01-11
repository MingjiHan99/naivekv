cmake_minimum_required(VERSION 3.10)
project(lsm_kv)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 17)

set (Boost_USE_STATIC_LIBS OFF)
set (CMAKE_CXX_FLAGS "-lstdc++fs -pthread")
find_package (Boost REQUIRED COMPONENTS unit_test_framework)
include_directories (${Boost_INCLUDE_DIRS})

link_libraries(stdc++fs)

aux_source_directory(src SRC)
add_executable(${PROJECT_NAME} ${SRC})
add_executable(naivedb_test ./test/t.cpp ${SRC})
add_executable(bloom_test ./test/test_bloom.cpp ${SRC} )
add_executable(compaction_test ./test/test_compaction.cpp ${SRC})
target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES})
target_link_libraries(naivedb_test ${Boost_LIBRARIES})
target_link_libraries(bloom_test ${Boost_LIBRARIES})
target_link_libraries(compaction_test ${Boost_LIBRARIES})