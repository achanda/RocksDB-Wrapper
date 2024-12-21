#include <rocksdb/db.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/table.h>

#include <chrono>
#include <condition_variable>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "config_options.h"

std::string kDBPath = "./db";
std::mutex mtx;
std::condition_variable cv;
bool compaction_complete = false;

void printExperimentalSetup(DBEnv* env);

class CompactionsListener : public EventListener {
public:
  explicit CompactionsListener() {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    auto localtp = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mtx);
    compaction_complete = true;
    cv.notify_one();
  }
};

void WaitForCompactions(DB* db) {
  std::unique_lock<std::mutex> lock(mtx);
  uint64_t num_running_compactions;
  uint64_t pending_compaction_bytes;
  uint64_t num_pending_compactions;

  while (!compaction_complete) {
    db->GetIntProperty("rocksdb.num-running-compactions", &num_running_compactions);
    db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compaction_bytes);
    db->GetIntProperty("rocksdb.compaction-pending", &num_pending_compactions);

    if (num_running_compactions == 0 && pending_compaction_bytes == 0 && num_pending_compactions == 0) {
      break;
    }
    cv.wait_for(lock, std::chrono::seconds(2));
  }
}

int runWorkload(DBEnv* env) {
  DB* db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options, &flush_options);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(kDBPath, options);
    std::cout << "Destroying database ..." << std::endl;
  }

  auto compaction_listener = std::make_shared<CompactionsListener>();
  options.listeners.emplace_back(compaction_listener);

  printExperimentalSetup(env);

  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return -1;
  }

  std::ifstream workload_file("workload.txt");
  if (!workload_file.is_open()) {
    std::cerr << "Failed to open workload file." << std::endl;
    return -1;
  }

  uint32_t workload_size = 0;
  std::string line;
  while (std::getline(workload_file, line)) {
    ++workload_size;
  }
  workload_file.close();

  if (env->clear_system_cache) {
    std::cout << "Clearing system cache ..." << std::endl;
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }

  if (env->IsPerfIOStatEnabled()) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_perf_context()->ClearPerLevelPerfContext();
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_iostats_context()->Reset();
    Options options;
    options.statistics = rocksdb::CreateDBStatistics();
  }

  workload_file.open("workload.txt");
  if (!workload_file.is_open()) {
    std::cerr << "Failed to reopen workload file." << std::endl;
    return -1;
  }

  auto it = db->NewIterator(read_options);
  uint32_t counter = 0;

  while (!workload_file.eof()) {
    char instruction;
    std::string key, start_key, end_key, value;
    workload_file >> instruction;

    switch (instruction) {
    case 'I':  // Insert
      workload_file >> key >> value;
      s = db->Put(write_options, key, value);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      ++counter;
      break;

    case 'U':  // Update
      workload_file >> key >> value;
      s = db->Put(write_options, key, value);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      ++counter;
      break;

    case 'D':  // Delete
      workload_file >> key;
      s = db->Delete(write_options, key);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      ++counter;
      break;

    case 'Q':  // Query
      workload_file >> key;
      s = db->Get(read_options, key, &value);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      ++counter;
      break;

    case 'S':  // Scan
      workload_file >> start_key >> end_key;
      it->Refresh();
      assert(it->status().ok());

      for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (it->key().ToString() >= end_key) {
          break;
        }
      }

      if (!it->status().ok()) {
        std::cerr << it->status().ToString() << std::endl;
      }
      ++counter;
      break;

    default:
      std::cerr << "ERROR: Unknown instruction." << std::endl;
      break;
    }
  }

  workload_file.close();

  std::vector<std::string> live_files;
  uint64_t manifest_size;
  db->GetLiveFiles(live_files, &manifest_size, true);
  WaitForCompactions(db);

  delete it;
  s = db->Close();
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
  }

  std::cout << "End of experiment - TEST !!" << std::endl;

  if (env->IsPerfIOStatEnabled()) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    std::cout << "RocksDB Perf Context: " << std::endl
      << rocksdb::get_perf_context()->ToString() << std::endl;
    std::cout << "RocksDB IO Stats Context: " << std::endl
      << rocksdb::get_iostats_context()->ToString() << std::endl;
    std::cout << "Rocksdb Stats: " << std::endl
      << options.statistics->ToString() << std::endl;
  }

  std::string db_get_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_CORE_JOULES);
  std::cout << "DB_GET_CORE_JOULES" << "\n" << db_get_core_joules << std::endl;

  std::string db_get_filter_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_FILTER_CORE_JOULES);
  std::cout  << "DB_GET_FILTER_CORE_JOULES" << "\n" << db_get_filter_core_joules << std::endl;

  std::string db_get_index_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_INDEX_CORE_JOULES);
  std::cout << "DB_GET_INDEX_CORE_JOULES" << "\n" << db_get_index_core_joules << std::endl;

  std::string db_get_disk_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_DISK_CORE_JOULES);
  std::cout << "DB_GET_DISK_CORE_JOULES" << "\n" << db_get_disk_core_joules << std::endl;

  return 1;
}

void printExperimentalSetup(DBEnv* env) {
  int l = 10;
  std::cout << std::setw(l) << "cmpt_sty"
    << std::setw(l) << "cmpt_pri"
    << std::setw(4) << "T"
    << std::setw(l) << "P"
    << std::setw(l) << "B"
    << std::setw(l) << "E"
    << std::setw(l) << "M"
    << std::setw(l) << "file_size"
    << std::setw(l) << "L1_size"
    << std::setw(l) << "blk_cch"
    << std::setw(l) << "BPK"
    << "\n";

  std::cout << std::setw(l) << env->compaction_style
    << std::setw(l) << env->compaction_pri
    << std::setw(4) << env->size_ratio
    << std::setw(l) << env->buffer_size_in_pages
    << std::setw(l) << env->entries_per_page
    << std::setw(l) << env->entry_size
    << std::setw(l) << env->GetBufferSize()
    << std::setw(l) << env->GetTargetFileSizeBase()
    << std::setw(l) << env->GetMaxBytesForLevelBase()
    << std::setw(l) << env->block_cache
    << std::setw(l) << env->bits_per_key
    << std::endl;
}
