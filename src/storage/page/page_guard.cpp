#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;
  latch_state_ = that.latch_state_;

  // invalidate "that" so that it segfault after moving
  // also tells the destructor not to call evict on the buffer
  Invalidate(&that);
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }

  // unlock everything here instead of in the wrapper class
  if (latch_state_ == 1) {
    page_->RUnlatch();
  } else if (latch_state_ == 2) {
    page_->WUnlatch();
  }

  // unpin page in the buffer pool is enough for the caller
  // the eviction is left for the buffer pool manager because
  // it needs to utilise LRU-K, so it may keep it
  assert(bpm_ != nullptr);
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

  Invalidate(this);  // Invalidate this page guard to avoid further page drop
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  assert(this != &that);

  Drop();  // the current page content is unused, drop it

  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;
  latch_state_ = that.latch_state_;

  Invalidate(&that);

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return ReadPageGuard(std::move(*this)); }
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return WritePageGuard(std::move(*this)); }

// NOLINT

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) { guard_.latch_state_ = 1; }
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { *this = std::move(that); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // only have one element in the class, move it
  this->guard_ = std::move(that.guard_);
  return *this;
}

ReadPageGuard::ReadPageGuard(BasicPageGuard &&basic) noexcept {
  // we did not use guard_ = basic, since the rvalue operator = would
  // drop the page held in basic

  guard_ = std::move(basic);
  ReadLatchPage();
}

void ReadPageGuard::ReadLatchPage() {
  if (guard_.page_ != nullptr) {
    // lock the page using read lock
    assert(guard_.bpm_ != nullptr);
    guard_.page_->RLatch();
    guard_.latch_state_ = 1;
  } else {
    guard_.latch_state_ = 0;
  }
}

void ReadPageGuard::Drop() {
  // unlock code is in the drop function
  guard_.Drop();
}

// call the destructor of guard_
ReadPageGuard::~ReadPageGuard() = default;

// NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) { guard_.latch_state_ = 2; }

WritePageGuard::WritePageGuard(BasicPageGuard &&basic) noexcept {
  guard_ = std::move(basic);
  WriteLatchPage();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { *this = std::move(that); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::WriteLatchPage() {
  if (guard_.page_ != nullptr) {
    // lock the page using write lock
    assert(guard_.bpm_ != nullptr);
    guard_.page_->WLatch();
    guard_.latch_state_ = 2;
  } else {
    guard_.latch_state_ = 0;
  }
}

void WritePageGuard::Drop() {
  guard_.Drop();  // would unlock the page there
}

// call the destructor of guard_
WritePageGuard::~WritePageGuard() = default;

// NOLINT

}  // namespace bustub
