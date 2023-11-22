#include "concurrency/watermark.h"
#include "common/exception.h"

namespace bustub {
auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  assert(read_ts >= watermark_);
  const auto itr = current_reads_.find(read_ts);
  if (itr == current_reads_.end()) {
    current_reads_.emplace(read_ts, 1);
  } else {
    ++itr->second;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  const auto itr = current_reads_.find(read_ts);
  if (itr == current_reads_.end()) {
    assert(0);
  }
  if (--itr->second <= 0) {
    const auto ts = itr->first;
    current_reads_.erase(itr);
    if (ts == watermark_) {
      // this is the last txn in the reading list, every txn
      // are now more recent than this ts.
      if (current_reads_.empty()) {
        // if this is the last active txn, just set it to the latest commit_ts
        // and any upcoming AddTxn would be >= commit_ts_, and would not change
        // the lowest ts, i.e. watermark
        // -> monotonic
        watermark_ = commit_ts_;
      } else {
        // if there are other active txn, set watermark to the lowest of them
        watermark_ = FindWaterMark();
      }
    }
  }
}
auto Watermark::FindWaterMark() const -> timestamp_t {
  // find the left most leaf of BST, O(logN)
  return current_reads_.begin()->first;
}

}  // namespace bustub
