//
// Created by Han Mingji on 2020/12/18.
//

#ifndef LSM_KV_NAIVE_DB_H
#define LSM_KV_NAIVE_DB_H

#include <experimental/filesystem>
#include <string>
#include <thread>
#include <cstdint>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include "mem_table.h"
#include "log_file.h"
#include "meta_data.h"
#include "file_wrapper.h"
#include "file_version.h"
#include "compactor.h"

namespace fs = std::experimental::filesystem;
#define MEMORY_LIMIT 1024 * 8 * 2
#define K 10
#define BIT_LENGTH 1000
class NaiveDB {
public:
    NaiveDB(std::string folder) {
        folder_ = std::move(folder);
        mem_table_ = new MemTable();
        imm_table_ = nullptr;
        log_file_ = nullptr;
        meta_data_ = new MetaData();
        meta_data_file_ = nullptr;
        file_version_ = new FileVersion();
        init();
        persist_imm_thread_ = new std::thread(&NaiveDB::persist_imm_thread, this);
        log_clean_thread_ = new std::thread(&NaiveDB::log_clean_thread, this);
        compact_thread_ = new std::thread(&NaiveDB::compact_thread, this);
    }

    ~NaiveDB() {
        end_thread_signal_.store(true);
        persist_imm_thread_->join();
        log_clean_thread_->join();
        compact_thread_->join();
        file_version_->dec_ref(false); // don't delete files of current version !!!!
        delete persist_imm_thread_;
        delete mem_table_;
        delete imm_table_;
        delete log_file_;
        if (meta_data_file_) {
            persist_meta_data();
            meta_data_file_->close();
        }
        delete meta_data_file_;
        meta_data_->del_ref();
    }

    void init();
    void put(std::string key, std::string value, bool removed =false);
    void del(std::string key);

    std::string get(std::string key);
    void persist_meta_data();
    void recover_meta_data();
    void replay_log();

private:
    const std::string meta_data_name_ = "METADATA";
    std::string folder_;
    std::fstream *meta_data_file_;
    std::atomic<bool> end_thread_signal_{false};
    std::thread *persist_imm_thread_;
    std::thread *log_clean_thread_;
    std::thread *compact_thread_;

    std::mutex mutex_;
    std::condition_variable condition_var_;
    MemTable *mem_table_;
    MemTable *imm_table_;
    FileVersion *file_version_;
    std::atomic<bool> has_imm_table_{false};
    MetaData *meta_data_;
    LogFile *log_file_;

    Compactor compactor_{LEVEL};



    void replay_log_in_table(const std::vector<LogItem>& items, MemTable *table);
    void load_file_index();
    void persist_imm_thread();
    void log_clean_thread();
    void compact_thread();
    std::string read_value_from_file(std::string key, FileVersion *file_version);
};


#endif //LSM_KV_NAIVE_DB_H
