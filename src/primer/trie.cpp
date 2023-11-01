#include "primer/trie.h"
#include <cassert>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!this->root_) {
    return nullptr;
  }
  auto p = this->root_;  // of type shared pointer
  for (const char &ch : key) {
    auto res = p->children_.find(ch);
    if (res == p->children_.end()) {
      // key not found
      return nullptr;
    }
    p = res->second;
  }
  if (!p->is_value_node_) {  // for internal nodes that might not have value
    return nullptr;
  }
  auto ptr = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(p);
  return ptr ? ptr->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> p;     // traversing pointer
  std::shared_ptr<TrieNode> pp;    // keep track of the parent pointer of p
  std::shared_ptr<TrieNode> root;  // keep track of the root of the new trie

  if (key.empty()) {
    // handle special case: empty key
    root = this->root_
               ? std::make_shared<TrieNodeWithValue<T>>(this->root_->children_, std::make_shared<T>(std::move(value)))
               : std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    return Trie(root);
  }

  p = pp = root =
      this->root_ ? std::shared_ptr<TrieNode>(std::move(this->root_->Clone())) : std::make_shared<TrieNode>();

  for (const char &ch : key) {
    // loop invariant: p is cloned, and can be modified
    // find its child node on the path
    pp = p;  // record the parent pointer
    auto child_itr = p->children_.find(ch);
    if (child_itr == p->children_.end()) {
      // if not found, create a new node that contains it
      // every node on the path after this node is going to be created as new
      auto child = std::make_shared<TrieNode>();
      p->children_.insert({ch, child});
      p = child;
    } else {
      // found its child, copy it to preserve the previous version
      auto child_ptr = std::shared_ptr<TrieNode>(std::move(child_itr->second->Clone()));
      p->children_[ch] = child_ptr;
      p = child_ptr;
    }
  }

  // this has to be a value node, be it leaf or internal node
  // because it is the end node in the path
  // NOTE: move non-copyable value use std::move, get a shared_ptr to a rvalue, use make_shared
  auto val_node_ptr = std::make_shared<TrieNodeWithValue<T>>(p->children_, std::make_shared<T>(std::move(value)));
  pp->children_[key.at(key.size() - 1)] = val_node_ptr;

  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  // special case: empty key
  if (key.empty()) {
    return this->root_->children_.empty() ? Trie() : Trie(std::make_shared<TrieNode>(this->root_->children_));
  }

  std::vector<std::pair<std::shared_ptr<const TrieNode>, char>> queue;
  std::shared_ptr<const TrieNode> root;
  std::shared_ptr<const TrieNode> p;
  p = this->root_;

  assert(p);
  // record the path
  for (const char &ch : key) {
    queue.emplace_back(p, ch);
    p = p->children_.at(ch);
  }

  // now p is the node to be deleted
  p = std::make_shared<TrieNode>(p->children_);

  if (p->children_.empty()) {  // p can be discarded, also
    while (!queue.empty()) {
      // while the node has no children, its path can be (partly) recycled
      // by discarding its removable parent
      auto &parent = queue.back().first;
      if (parent->children_.size() != 1 || parent->is_value_node_) {
        // its parent has other children or has value
        // meaning the path above its parent cannot be altered
        // now point to the parent
        auto cloned_parent = std::shared_ptr<TrieNode>(parent->Clone());  // clone the parent
        cloned_parent->children_.erase(
            queue.back().second);  // erase its children so that it will not reference to the deleted path
        p = cloned_parent;
        queue.pop_back();  // pop the parent
        break;
      } else {
        // its parent has only one child (i.e. itself)
        // it the parent can be ignored
        queue.pop_back();
      }
    }
  }

  if (queue.empty()) {
    // the trie is cleared if the queue is empty
    // now p is the root
    return Trie(p);
  }

  // if the deleted node has children, meaning that
  // nodes on the path are important, we simply construct
  // a new tree

  while (!queue.empty()) {
    // clone the parent
    auto new_parent = std::shared_ptr<TrieNode>(queue.back().first->Clone());
    new_parent->children_[queue.back().second] = p;  // renew its pointer to new child node
    p = new_parent;
    queue.pop_back();
  }

  assert(p);

  // work backwards to the top, hence p is now the root
  return Trie(p);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
