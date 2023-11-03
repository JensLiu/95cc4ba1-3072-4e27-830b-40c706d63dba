//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {}

void LRUKNode::Access(size_t current_ts) {
  if (history_.size() + 1 > k_) {
    // if exceeds K value, remove the oldest history record
    history_.pop_back();
  }
  history_.push_front(current_ts);
}

auto LRUKNode::Evictable() -> bool { return is_evictable_; }
void LRUKNode::SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }
auto LRUKNode::LeastRecentTimestamp() -> size_t {
  // the reason for LRU-K to use the least recent timestamp to evict frame
  // rather than most recent timestamp
  // is that recently accessed page may not be accessed again
  return history_.back();
}
auto LRUKNode::FrameId() -> frame_id_t { return fid_; }
auto LRUKNode::BackwardKDistance(size_t current_ts) -> size_t {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return current_ts - history_.front();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  size_t pos_inf = std::numeric_limits<size_t>::max();
  size_t least_recent_ts = pos_inf;  // only used when degraded to LRU-1
  frame_id_t evict_frame_id = -1;
  size_t greatest_k_dist = 0;

  for (auto &pair : node_store_) {
    LRUKNode &frame = pair.second;

    if (!frame.Evictable()) {  // skip unevictable frame
      continue;
    }

    // evictable frame
    size_t k_dist = frame.BackwardKDistance(current_timestamp_);

    if (k_dist < greatest_k_dist) {  // skip frames with less backward k distance
      continue;
    }

    // k_dist >= greatest_k_dist: page could potentially be evicted
    if (k_dist != pos_inf) {            // k_dist < pos_inf (could not be greater): use LRU-K
      if (k_dist >= greatest_k_dist) {  // only the case: greatest_k_dist <= k_dist < pos_inf
        evict_frame_id = frame.FrameId();
        greatest_k_dist = k_dist;
      }

    } else {  // +inf backward k-distance
      size_t ts = frame.LeastRecentTimestamp();

      if (greatest_k_dist == pos_inf) {  // more than one pos_inf frame: using classical LRU algorithm (LRU-1)
        if (ts < least_recent_ts) {      // should we test for equality ?
          // the frame has NOT been called recently, thus with less timestamp
          evict_frame_id = frame.FrameId();
          least_recent_ts = ts;
        }
      } else {  // greatest_k_dist < pos_inf: the first time that k_dist reaches +inf: stop using LRU-K after this point
        greatest_k_dist = pos_inf;  // = k_dist
        evict_frame_id = frame.FrameId();
        least_recent_ts = ts;
      }
    }
  }

  if (evict_frame_id != -1) {
    node_store_.erase(evict_frame_id);
    curr_size_--;
    *frame_id = evict_frame_id;
    assert(node_store_.find(evict_frame_id) == node_store_.end());
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);
  //  std::cout << "set access frame: " << frame_id << "\n";

  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {  // invalid frame id
    throw Exception("LRUKReplacer::RecordAccess: invalid frame id");
  }

  current_timestamp_++;

  auto itr = node_store_.find(frame_id);
  if (itr == node_store_.end()) {  // not in page
    auto frame_node = LRUKNode(frame_id, k_);
    frame_node.Access(current_timestamp_);
    node_store_.insert({frame_id, frame_node});
  } else {
    itr->second.Access(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);
  //  std::cout << "set ecivtable frame: " << frame_id << ", evictable " << set_evictable << "\n";

  auto itr = node_store_.find(frame_id);
  if (itr == node_store_.end()) {
    throw Exception("LRUKReplacer::SetEvictable: invalid frame id!");
  }

  bool frame_evictable = itr->second.Evictable();
  if (!frame_evictable && set_evictable) {
    curr_size_++;
  } else if (frame_evictable && !set_evictable) {
    curr_size_--;
  }
  itr->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  //  std::cout << "remove frame: " << frame_id << "\n";

  auto itr = node_store_.find(frame_id);
  if (itr == node_store_.end()) {
    return;
  }
  if (itr->second.Evictable()) {
    node_store_.erase(itr);
    curr_size_--;
  } else {
    throw Exception("LRUKReplacer::Remove: evicting an unevictable frame!");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);  // is a lock necessary?
  return curr_size_;
}

}  // namespace bustub
