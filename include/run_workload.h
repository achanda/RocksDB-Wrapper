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
#include <numeric>

#include "config_options.h"

std::string kDBPath = "./db";
std::mutex mtx;
std::condition_variable cv;
bool compaction_complete = false;

void printExperimentalSetup(DBEnv* env, uint32_t workload_size);

void printLSM() {
    DB* db;
    Options options;

    Status status = rocksdb::DB::Open(options, kDBPath, &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return;
    }
    if (db == nullptr) {
        std::cerr << "Error: DB pointer is null" << std::endl;
        return;
    }

    // Get the default column family handle
    rocksdb::ColumnFamilyHandle* default_cf = db->DefaultColumnFamily();

    // Retrieve metadata for the default column family
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(default_cf, &cf_meta);

    std::cout << "LSM-tree structure:" << std::endl;
    std::cout << "-------------------" << std::endl;

    // Iterate through each level and print detailed information
    for (const auto& level : cf_meta.levels) {
        uint64_t level_element_count = 0;
        uint64_t level_size = 0;

        std::cout << "Level " << level.level << ":" << std::endl;

        std::cout << "Total for Level " << level.level
                  << ": " << level_element_count << " elements, "
                  << level_size << " bytes" << std::endl;
        std::cout << std::endl;
    }

    // Print total element count across all levels
    uint64_t total_elements = std::accumulate(
        cf_meta.levels.begin(), cf_meta.levels.end(), 0ULL,
        [](uint64_t sum, const rocksdb::LevelMetaData& level) {
            return sum + std::accumulate(
                level.files.begin(), level.files.end(), 0ULL,
                [](uint64_t file_sum, const rocksdb::SstFileMetaData& file) {
                    return file_sum + file.num_entries;
                }
            );
        }
    );

    std::cout << "-------------------" << std::endl;
    std::cout << "Total elements across all levels: " << total_elements << std::endl;

    Status s = db->Close();
    if (!s.ok()) {
      std::cerr << s.ToString() << std::endl;
      return;
    }
}

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

// Configure RocksDB options for bulk loading
rocksdb::Options GetBulkLoadOptions() {
    rocksdb::Options options;
    
    // Memory and write optimization
    options.IncreaseParallelism(std::thread::hardware_concurrency());
    options.OptimizeLevelStyleCompaction();
    options.write_buffer_size = 256 * 1024 * 1024;  // 256MB
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 1;
    
     options.create_if_missing = true;
    // Disable features that slow down bulk loading
    options.compression = rocksdb::kNoCompression;
    options.level0_file_num_compaction_trigger = (1 << 30);
    options.level0_slowdown_writes_trigger = (1 << 30);
    options.level0_stop_writes_trigger = (1 << 30);
    
    // File system optimization
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
    
    return options;
}

int BulkLoad(const std::string& filename) {
    constexpr size_t BATCH_SIZE = 500000;  // Increased batch size for bulk load
    rocksdb::WriteOptions write_options;

     DB* db;
   
     //options.PrepareForBulkLoad();
    Status status = rocksdb::DB::Open(GetBulkLoadOptions(), kDBPath, &db);
    //Status status = rocksdb::DB::Open(options, kDBPath, &db);
    if (!status.ok()) {
    	std::cerr << status.ToString() << std::endl;
    	return -1;
    }

    // Critical performance options
    write_options.sync = false;
    write_options.disableWAL = true;
    write_options.ignore_missing_column_families = false;
    write_options.no_slowdown = true;

    std::ifstream file(filename);
    if (!file) return -1;

    rocksdb::WriteBatch batch;
    size_t count = 0;
    std::string line;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string op, key, value;
        
        // Parse line with format "I <key> <value>"
        if (!(iss >> op >> key >> value) || op != "I") continue;
        
        // Verify no extra data in line
        std::string extra;
        if (iss >> extra) continue;

        batch.Put(key, value);
        
        if (++count % BATCH_SIZE == 0) {
            if (auto s = db->Write(write_options, &batch); !s.ok()) return -1;
            batch.Clear();
        }
    }

    // Flush remaining entries
    if (count % BATCH_SIZE != 0) {
        if (auto s = db->Write(write_options, &batch); !s.ok()) return -1;
    }


    Status s = db->Close();
    if (!s.ok()) {
      std::cerr << s.ToString() << std::endl;
      return 1;
    }
    return 0;
}

int runWorkload(DBEnv* env) {
  DB* db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options, &flush_options);

  /*if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(kDBPath, options);
    std::cout << "Destroying database ..." << std::endl;
  }*/

  auto compaction_listener = std::make_shared<CompactionsListener>();
  options.listeners.emplace_back(compaction_listener);

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

  printExperimentalSetup(env, workload_size);
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
  //printLSM(db);

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

void printExperimentalSetup(DBEnv* env, uint32_t workload_size) {
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
    << std::setw(l) << "WSZ" //workload size
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
    << std::setw(l) << workload_size
    << std::endl;
}
