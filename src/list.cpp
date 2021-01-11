//
// Created by Han Mingji on 2020/12/20.
//
#include "list.h"

void List::insert_node(ListNode *node) {
    auto p = head;
    while (p->next_[level_] != tail && node->key_ > p->next_[level_]->key_)  {
        p = p->next_[level_];
    }
    auto temp = p->next_[level_];
    p->next_[level_] = node;
    node->next_[level_] = temp;

}


void List::delete_node(std::string key) {
    auto p = head;
    while (p->next_[level_] != tail && key > p->next_[level_]->key_)  {
        p = p->next_[level_];
    }

    if (p->next_[level_] != tail && p->next_[level_]->key_ == key) {
        p->next_[level_] = p->next_[level_]->next_[level_];
    }
}


ListNode* List::find_node(std::string key) {
    auto p = head;
    while (p != tail && p->key_ != key) {
        p = p->next_[level_];
    }
    if (p == tail) {
        return nullptr;
    }
    return p;
}



void List::print_debug() {

    auto p = head->next_[level_];
    while (p != tail) {
        std::cout << p->key_ << " ";
        p = p->next_[level_];
    }
    std::cout << "\n";
}

