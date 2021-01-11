//
// Created by Han Mingji on 2021/1/6.
//

#ifndef LSM_KV_FILE_VERSION_H
#define LSM_KV_FILE_VERSION_H

#include <vector>
#include <cassert>
#include "file_wrapper.h"
#define LEVEL 3

struct FileVersion {
    FileVersion() {
        ref_ = 1;
    }
    void add_ref() {
        ref_++;
    }

    void dec_ref(bool remove_file) {
        assert(ref_ >= 1);

        ref_--;

        if (ref_ <= 0) {
            for (auto & file : files_) {
                for (auto wrapper : file) {
                    wrapper->dec_ref(remove_file);
                }
            }
            delete this;
        }

    }
    FileVersion(const FileVersion& file_version)  {
        for (int i = 0; i < LEVEL; i++) {
            for (int j = 0;j < file_version.files_[i].size(); j++) {
                FileWrapper *wrapper = file_version.files_[i][j];
                wrapper->add_ref();
                files_[i].push_back(wrapper);
            }
        }
    }

    std::vector<FileWrapper*> files_[LEVEL];
private:
    int ref_;
};


#endif //LSM_KV_FILE_VERSION_H
