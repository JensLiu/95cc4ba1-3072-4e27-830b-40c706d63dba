//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  Page *page = FindOneReusablePage(&frame_id);

  if (page == nullptr) {  // no way of finding the page, return nullptr
    return nullptr;
  }

  // NOTE: when asking about a new page, it should return a brand new empty page whose
  // page id is unique to all previous ones! We can reuse frame id (position in the buffer)
  // but not page id since they are actually files in the disk with unique ids.
  page->page_id_ = AllocatePage();

  // by now, there is a frame available
  page_table_.emplace(page->page_id_, frame_id);  // update page table entry

  *page_id = page->page_id_;  // update page_id of the caller

  page->pin_count_ = 1;                      // pin the page because now the caller is using it
  replacer_->RecordAccess(frame_id);         // add to replacer
  replacer_->SetEvictable(frame_id, false);  // disable eviction
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // invariant: page table should always be mapped to frame that has valid data
  // i.e. pages in page pool should always be up-to-date.
  std::scoped_lock<std::mutex> lock(latch_);

  Page *page = PageId2Page(page_id);
  frame_id_t frame_id = -1;

  // fast: page in the buffer pool
  if (page != nullptr) {
    frame_id = PageId2FrameId(page_id);
    page->pin_count_++;                 // record access from a worker (how to make sure each worker call exactly once?)
    replacer_->RecordAccess(frame_id);  // record access in the replacer
    replacer_->SetEvictable(frame_id, false);  // disable eviction
    return page;
  }

  // slow: page not in the buffer pool, try to evict one
  page = FindOneReusablePage(&frame_id);

  if (page == nullptr) {
    return nullptr;
  }
  assert(frame_id != -1);

  // initialisation
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  FetchPageFromDisk(page);                   // fetch page from the disk, expensive operation
  replacer_->RecordAccess(frame_id);         // add to replacer
  replacer_->SetEvictable(frame_id, false);  // disable eviction

  // update page table
  page_table_.emplace(page->page_id_, frame_id);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  Page *page = PageId2Page(page_id);
  if (page == nullptr || page->pin_count_ == 0) {
    return false;
  }
  assert(page->pin_count_ > 0);
  if (is_dirty) {  // if one worker modified the page, it is dirty until it is flushed
    page->is_dirty_ = is_dirty;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(PageId2FrameId(page_id), true);  // unpin from the frame buffer, can be evicted if necessary
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  Page *page = PageId2Page(page_id);
  if (page == nullptr) {
    return false;
  }

  return FlushPageToDisk(page);
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageToDisk(&pages_[i]);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  Page *page = PageId2Page(page_id);
  if (page == nullptr) {  // page not in the buffer pool
    return true;
  }
  frame_id_t frame_id = PageId2FrameId(page_id);
  if (page->pin_count_ != 0) {
    return false;
  }
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);  // stop tracking the page

  free_list_.push_front(frame_id);    // recycle frame id
  page_table_.erase(page->page_id_);  // remove its page table entry
  FrameReset(page);                   // it wipes the frame data for later use
  DeallocatePage(page->page_id_);

  assert(PageId2Page(page_id) == nullptr);
  assert(pages_[frame_id].page_id_ == INVALID_PAGE_ID);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  return {this, page};
}
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  BasicPageGuard basic = FetchPageBasic(page_id);
  return basic.UpgradeRead();
  //  return FetchPageBasic(page_id).UpgradeRead();
}
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  return FetchPageBasic(page_id).UpgradeWrite();
}
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  return {this, page};
}

// helper functions: caller should hold a latch

auto BufferPoolManager::FlushPageToDisk(Page *page) -> bool {
  assert(page != nullptr);
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, /* is_write_ */
                             page->data_, page->page_id_, std::move(promise)});
  future.wait();
  page->is_dirty_ = false;  // the flushed page's dirty flag is cleared
  return true;
}

auto BufferPoolManager::FetchPageFromDisk(Page *page) -> bool {
  assert(page != nullptr);
  assert(!page->is_dirty_);
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, /* is_write_ */
                             page->data_, page->page_id_, std::move(promise)});
  future.wait();
  page->is_dirty_ = false;  // page fetched from disk is never dirty since no one modifies it at the point
  return true;
}

auto BufferPoolManager::EvictOnePage(frame_id_t *frame_id) -> Page * {
  if (!replacer_->Evict(frame_id)) {
    return nullptr;
  }

  // evicted page write back
  Page *page = FrameId2Page(*frame_id);
  if (page->is_dirty_) {
    FlushPageToDisk(page);
  }
  page_table_.erase(page->page_id_);  // remove its page table entry
  FrameReset(page);                   // it removes page table entry

  return page;
}

void BufferPoolManager::FrameReset(Page *page) {
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
}

auto BufferPoolManager::PageId2Page(page_id_t page_id) -> Page * {
  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return nullptr;
  }
  auto frame_id = itr->second;
  assert(pages_[frame_id].page_id_ == page_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::FrameId2Page(frame_id_t frame_id) -> Page * { return &pages_[frame_id]; }

auto BufferPoolManager::PageId2FrameId(page_id_t page_id) -> frame_id_t {
  auto itr = page_table_.find(page_id);
  assert(itr != page_table_.end());
  return itr->second;
}

auto BufferPoolManager::FindOneReusablePage(frame_id_t *frame_id) -> Page * {
  frame_id_t frame_id_tmp;
  Page *page = FindAvailableFrameInPool(&frame_id_tmp);  // check for unused frames in the pool
  if (page == nullptr) {
    // if no free frames, ask replacer to evict one
    page = EvictOnePage(&frame_id_tmp);
  }
  if (page != nullptr) {
    *frame_id = frame_id_tmp;
    return page;
  }
  return nullptr;
}

auto BufferPoolManager::FindAvailableFrameInPool(frame_id_t *frame_id) -> Page * {
  if (free_list_.empty()) {
    return nullptr;
  }
  frame_id_t frame_id_tmp = free_list_.front();
  free_list_.pop_front();  // remember to remove the frame id from free list
  Page *page = FrameId2Page(frame_id_tmp);
  assert(page->page_id_ == INVALID_PAGE_ID);
  *frame_id = frame_id_tmp;
  return page;
}

}  // namespace bustub
