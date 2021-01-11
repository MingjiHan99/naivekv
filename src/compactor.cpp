//
// Created by Han Mingji on 2021/1/7.
//


#include "compactor.h"

// sort all items in the files in level and time order
MemTable* Compactor::merge_sort(const std::vector<std::string>& file_names) {
    std::priority_queue<SortItem> pq;
    for (auto& name: file_names) {
        SSTable table{name};
        uint64_t level = name.at(name.size() - 1) - '0';
        auto& kv_map = table.get_kv();

        for (auto& kv: kv_map) {
            pq.push(SortItem{kv.first, kv.second.first, kv.second.second, level});
        }
    }
    MemTable *table = new MemTable();


    while (!pq.empty()) {
        SortItem top_item = pq.top();
        pq.pop();
        while (!pq.empty() && top_item.key_ == pq.top().key_) {
            top_item = pq.top();
            pq.pop();
        }
        table->put(top_item.time_stamp_, top_item.key_, top_item.value_);
    }

    return table;
}

// compress multiple files into one table
std::pair<MemTable*, std::vector<std::string>> Compactor::l0_compact(FileVersion* file_version) {

    auto& l0_files = file_version->files_[0];
    if (l0_files.empty()) {
        return make_pair(nullptr, std::vector<std::string>());
    }
    // file the file we want to compact
    auto& min_key = last_merged_keys_[0];
    FileWrapper* target_file = nullptr;

    for (auto& file: l0_files) {
        // the first one
        if (!target_file) {
            target_file = file;
        }
        else if (last_merged_keys_[0] <= file->get_file_name() && file->get_file_name() <= target_file->get_file_name()) {
            target_file = file;
        }
    }
    assert(target_file != nullptr);
    // find all file names for merge sort
    auto target_l0_files = find_intersect_files(target_file->get_min_key(), target_file->get_max_key(), 0, file_version);
    auto target_l1_files = find_intersect_files(target_file->get_min_key(), target_file->get_max_key(), 1, file_version);

    target_l0_files.insert(target_l0_files.begin(), target_l1_files.begin(), target_l1_files.end());

    MemTable* mem_table = merge_sort(target_l0_files);

    last_merged_keys_[0] = target_file->get_min_key();
    return make_pair(mem_table, target_l0_files);
}

std::vector<std::string> Compactor::find_intersect_files(std::string min_key, std::string max_key, int level, FileVersion *file_version) {
    std::vector<std::string> result;
    for (auto& wrapper: file_version->files_[level]) {
        auto file_max_key = wrapper->get_max_key();
        auto file_min_key = wrapper->get_min_key();
        if ( (file_min_key <= min_key && min_key <= file_max_key) || (file_min_key <= max_key && max_key <= file_max_key)) {
            result.emplace_back(wrapper->get_file_name());
        }
    }

    return result;
}
