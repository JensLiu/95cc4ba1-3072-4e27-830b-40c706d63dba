//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // the Channel ensures thread safety
  request_queue_.Put(std::make_optional<DiskRequest>(std::move(r)));
}

void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> opt_req;
  // the Channel ensures thread safety
  while ((opt_req = request_queue_.Get()).has_value()) {
    if (opt_req.has_value()) {  // clang bug, otherwise won't pass clang-tidy
      DiskRequest &req = opt_req.value();
      if (req.is_write_) {
        disk_manager_->WritePage(req.page_id_, req.data_);
      } else {
        disk_manager_->ReadPage(req.page_id_, req.data_);
      }
      req.callback_.set_value(true);
    }
  }
}

}  // namespace bustub
