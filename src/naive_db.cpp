//
// Created by Han Mingji on 2020/12/18.
//

#include "naive_db.h"

#include <utility>
#include "sstable.h"

void NaiveDB::put(std::string key, std::string value, bool removed) {

    std::unique_lock<std::mutex> lock(mutex_);
    if (mem_table_->get_memory_usage() > MEMORY_LIMIT) {
        std::cout << "Start dumping \n";
        if (imm_table_ != nullptr) {
            condition_var_.wait(lock);
        }
        // new imm table
        imm_table_ = mem_table_;
        has_imm_table_.store(true);
        meta_data_->is_imm_table_ = true;
        // new log
        delete log_file_;
        log_file_ = new LogFile(folder_ + std::to_string(meta_data_->next_log_number_) + ".LOG");
        meta_data_->next_log_number_++;
        std::cout << "Log id " << meta_data_->next_log_number_ << std::endl;
        // new mem table
        mem_table_ = new MemTable();
        // We should update meta data in the last write step, otherwise we will read invalid file should be existed in meta data.
        persist_meta_data();
    }
    uint64_t time_stamp = std::time(nullptr);
    log_file_->write_log(time_stamp, removed ? Operation::DELETE : Operation::PUT, key, value);
    mem_table_->put(time_stamp, key, value);
  //  std::cout << "File size :" << mem_table_->get_memory_usage() <<  '\n';
}

void NaiveDB::del(std::string key) {
    put(std::move(key), "", true);
}

std::string NaiveDB::get(std::string key) {
    std::unique_lock<std::mutex> lock(mutex_);

    MemTable *mem_table = mem_table_;
    mem_table->add_ref();

    MemTable *imm_table = imm_table_;
    if (imm_table) {
        imm_table->add_ref();
    }

    FileVersion *file_version = file_version_;
    file_version->add_ref();


    lock.unlock();

    std::string result;
    bool found = false;
    // first we search in memory table
    // mem_table->display_all_list();
    ListNode *ptr_in_mem_table = mem_table->get(key);

    if (ptr_in_mem_table) {
        found = true;
        result = ptr_in_mem_table->value_;
    //    std::cout << "Get from mem" << std::endl;
    }

    // second we search in immutable table
    if (!found && imm_table) {
        ListNode *ptr_in_imm_table = imm_table->get(key);
        if (ptr_in_imm_table) {
            found = true;
            result = ptr_in_imm_table->value_;
       //       std::cout << "Get from imm" << std::endl;
        }
    }

    if (!found) {
        result = read_value_from_file(key, file_version);
    }

    lock.lock();


    mem_table->del_ref();

    if (imm_table) {
        imm_table->del_ref();
    }

    file_version->dec_ref(true);

    return result;
}

std::string NaiveDB::read_value_from_file(std::string key, FileVersion *file_version) {
    uint64_t max_time_stamp = 0;
    std::string result = "";
    for (auto & file : file_version->files_) {
        std::cout << file.size() << std::endl;
        for (int j = file.size() - 1; j >= 0; j--) {
           std::cout << "Read from file:" << file[j]->get_file_name() << " " << file[j]->get_min_key()
            << " " << file[j]->get_max_key() << std::endl;
            if (file[j]->get_min_key() <= key && key <= file[j]->get_max_key()) {
                SSTable table{file[j]->get_file_name()};

                auto [value, exist, time_stamp] = table.get(key);

                if (exist && time_stamp > max_time_stamp) {
                    max_time_stamp = time_stamp;
                    result = value;
                  std::cout << "Get from file:" << file[j]->get_file_name() << std::endl;
                }
            }
        }
    }

    return result;
}

void NaiveDB::persist_imm_thread() {
    while (true) {
        bool is_terminated = end_thread_signal_.load();
        if (is_terminated) {

           break;
        }
        if (has_imm_table_.load()) {
            std::lock_guard<std::mutex> lock(mutex_);

            // persist immutable memory table
            std::ofstream out(folder_ + std::to_string(meta_data_->l0_sst_next_) + ".L0", std::ios::binary | std::ios::trunc);
            imm_table_->dump_to_file(out);
            out.close();

            file_version_->files_[0].push_back(new FileWrapper(folder_ + std::to_string(meta_data_->l0_sst_next_) + ".L0"));
            // update variables
            imm_table_->del_ref();
            imm_table_ = nullptr;
            has_imm_table_.store(false);
            meta_data_->is_imm_table_ = true;
            meta_data_->l0_sst_next_++;
            // persist meta_data
            persist_meta_data();

        }
        condition_var_.notify_all();
    }
}

void merge_sstable_thread() {

}

void NaiveDB::init() {
    // init stage, no lock is required
    std::fstream meta_data_file(folder_ + meta_data_name_);
    std::cout << folder_ + meta_data_name_ << std::endl;
    // if there is no meta data, it is a new database
    if(!meta_data_file) {
        std::cout << "Creating a new database..." << std::endl;
        std::ofstream new_file(folder_ + meta_data_name_);
        meta_data_file_ = new std::fstream(folder_ + meta_data_name_, std::ios::binary | std::ios::in | std::ios::out);
        meta_data_->l1_sst_start_ = 0;
        meta_data_->l1_sst_end_ = 0;
        meta_data_->l0_sst_start_ = 0;
        meta_data_->l0_sst_next_ = 0;
        meta_data_->next_log_number_ = 1;
        meta_data_->is_imm_table_ = false;
        log_file_ = new LogFile(folder_ + "0.LOG");
        persist_meta_data();
    } else {
        meta_data_file_ = new std::fstream(folder_ + meta_data_name_, std::ios::binary | std::ios::in | std::ios::out);
        recover_meta_data();
        replay_log();
        load_file_index();

    }


}

void NaiveDB::recover_meta_data() {

    meta_data_file_->read(reinterpret_cast<char*>(&meta_data_->next_log_number_), sizeof(uint64_t));
    uint64_t is_imm_table_uint64_ = meta_data_->is_imm_table_ ? 1 : 0;
    meta_data_file_->read(reinterpret_cast<char*>(&is_imm_table_uint64_), sizeof(uint64_t));
    meta_data_file_->read(reinterpret_cast<char*>(&meta_data_->l0_sst_start_), sizeof(uint64_t));
    meta_data_file_->read(reinterpret_cast<char*>(&meta_data_->l0_sst_next_), sizeof(uint64_t));
    meta_data_file_->read(reinterpret_cast<char*>(&meta_data_->l1_sst_start_), sizeof(uint64_t));
    meta_data_file_->read(reinterpret_cast<char*>(&meta_data_->l1_sst_end_), sizeof(uint64_t));
    meta_data_file_->clear();

}

void NaiveDB::persist_meta_data() {
    meta_data_file_->seekp(0);
    meta_data_file_->write(reinterpret_cast<char*>(&meta_data_->next_log_number_) , sizeof(uint64_t));
    meta_data_file_->seekp(8);
    uint64_t is_imm_table_uint64_ = 0;
    meta_data_file_->write(reinterpret_cast<char*>(&is_imm_table_uint64_) ,sizeof(uint64_t));
    meta_data_->is_imm_table_ = is_imm_table_uint64_ == 1;
    meta_data_file_->seekp(16);
    meta_data_file_->write(reinterpret_cast<char*>(&meta_data_->l0_sst_start_) ,sizeof(uint64_t));
    meta_data_file_->seekp(24);
    meta_data_file_->write(reinterpret_cast<char*>(&meta_data_->l0_sst_next_) , sizeof(uint64_t));
    meta_data_file_->seekp(32);
    meta_data_file_->write(reinterpret_cast<char*>(&meta_data_->l1_sst_start_) ,sizeof(uint64_t));
    meta_data_file_->seekp(40);
    meta_data_file_->write(reinterpret_cast<char*>(&meta_data_->l1_sst_end_) ,sizeof(uint64_t));
}

void NaiveDB::replay_log() {
    // replay for mem table
    log_file_ = new LogFile(folder_ + std::to_string(meta_data_->next_log_number_ - 1) + ".LOG");
    replay_log_in_table(log_file_->parse_log(), mem_table_);

    // replay for imm memtable
    if (meta_data_->is_imm_table_) {
        LogFile imm_log_file(folder_ + std::to_string(meta_data_->next_log_number_ - 2) + ".LOG");
        imm_table_ = new MemTable();
        replay_log_in_table(imm_log_file.parse_log(), imm_table_);
    }
}

void NaiveDB::replay_log_in_table(const std::vector<LogItem>& items, MemTable *table) {
  //  std::cout << "Item size:" << items.size() << std::endl;
    for (LogItem item: items) {
      //  std::cout << "k: " << item.key_ << " v:" << item.value_ << std::endl;
        if (item.op_ == Operation::PUT) {
            table->put(item.time_stamp_, item.key_, item.value_);
        } else if (item.op_ == Operation::DELETE) {
            table->put(item.time_stamp_, item.key_, "");
        }
    }
    std::cout << "Replay done\n";
}

// clean log periodly
void NaiveDB::log_clean_thread() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        bool is_terminated = end_thread_signal_.load();
        if (is_terminated) {
            break;
        }

        uint64_t next_log_num = meta_data_->next_log_number_;
        if (next_log_num >= 3) {
            for (auto& p: fs::directory_iterator(folder_)) {
                if(p.path().extension() == ".LOG") {
                    std::string path_str = p.path().string();
                    size_t first_index = path_str.find_last_of('/');
                    size_t last_index = path_str.find_last_of('.');
                    std::string file_id = path_str.substr(first_index + 1, last_index);
                    int log_id = std::stoi(file_id);
                    if (log_id <= next_log_num - 3) {
                        fs::remove(p);
                    }
                }
            }
        }

    }
}

void NaiveDB::load_file_index() {
    std::cout << "start loading files" << std::endl;
    for (auto& p: fs::directory_iterator(folder_)) {
        std::cout << p.path().string() << std::endl;
        std::cout << p.path().extension().string() << std::endl;
        if (p.path().extension().string() == ".L0") {

            FileWrapper* wrapper = new FileWrapper(p.path().string());
            std::cout << "Load l0 " << " min key: " << wrapper->get_min_key() << "max key: "  << wrapper->get_max_key() << std::endl;
            file_version_->files_[0].push_back(wrapper);
        }
        else if (p.path().extension().string() == ".L1") {
            FileWrapper* wrapper = new FileWrapper(p.path().string());
            std::cout << "Load l1 " << " min key: " << wrapper->get_min_key() << "max key: "  << wrapper->get_max_key() << std::endl;
            file_version_->files_[1].push_back(wrapper);
        }
    }

}

void NaiveDB::compact_thread() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        bool is_terimnated = this->end_thread_signal_;
        if (is_terimnated) {
            return ;
        }

        std::unique_lock<std::mutex> lock{mutex_};
        FileVersion *file_version = file_version_;
        file_version->add_ref();

        lock.unlock();
        if (file_version->files_[0].size() > 5) {


            auto [table, file_list] = compactor_.l0_compact(file_version);
            // TODO: Refactor, it just works but is not efficient
            if (table) {
                lock.lock();
                auto new_version = new FileVersion{*file_version};
                for (auto iter = new_version->files_[0].begin(); iter != new_version->files_[0].end(); ) {
                    for (auto& name: file_list) {
                        if (name == (*iter)->get_file_name()) {
                            (*iter)->dec_ref(true);
                            new_version->files_[0].erase(iter);
                        }
                    }
                    iter++;
                }


                uint64_t file_id = meta_data_->l1_sst_end_++;
                std::string new_file_name = folder_ + std::to_string(file_id) + ".L1";
                std::ofstream os{new_file_name, std::ios::binary | std::ios::trunc};
                table->dump_to_file(os);
                os.close();
                new_version->files_[1].push_back(new FileWrapper(new_file_name));

                file_version->dec_ref(true);
                FileVersion *old = file_version_;
                file_version_ = new_version;
                old->dec_ref(true);
                table->del_ref();

                persist_meta_data();
            }
        }
    }

}





