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

        // Iterate through files in the level and sum up entries and sizes
        for (const auto& file : level.files) {
            level_element_count += file.num_entries;
            level_size += file.size;
        }

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

int BulkLoad(const std::string& filename, const std::string& db_path) {
    // Create and configure Options
    Options options;
    options.create_if_missing = true;
    options.statistics = CreateDBStatistics();
    options.target_file_size_base = 1024 * 1024 * 1024;
    options.write_buffer_size = options.target_file_size_base;
    options.max_bytes_for_level_base = options.target_file_size_base;
    options.level0_file_num_compaction_trigger = 2;
    options.target_file_size_multiplier = 1;
    options.compression = rocksdb::kNoCompression;

    // Configure table options
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // Open the database
    DB* db;
    Status s = DB::Open(options, db_path, &db);
    if (!s.ok()) {
	std::cout << "Could not open rocksdb: " << s.ToString() << std::endl;
        return -1;
    }

    // Prepare for bulk loading
    SstFileWriter sst_file_writer(EnvOptions(), options);
    std::string tmp_file = filename + ".sst";
    s = sst_file_writer.Open(tmp_file);
    if (!s.ok()) {
        delete db;
	std::cout << "Could not open sst file: " << s.ToString() << std::endl;
        return -1;
    }

    // Read data from the input file and add to SST file writer
    std::ifstream input_file(filename);
    std::string line;
    std::vector<std::pair<std::string, std::string>> key_value_pairs;

    while (std::getline(input_file, line)) {
      std::istringstream iss(line);
      std::string indicator, key, value;
      if (iss >> indicator >> key >> value) {
        if (indicator == "I") {
            key_value_pairs.emplace_back(key, value);
        }
      }
    }

    std::sort(key_value_pairs.begin(), key_value_pairs.end());

    for (const auto& pair : key_value_pairs) {
      s = sst_file_writer.Put(pair.first, pair.second);
      if (!s.ok()) {
        sst_file_writer.Finish();
        delete db;
        return -1;
      }
   }

    // Finish writing to the SST file
    s = sst_file_writer.Finish();
    if (!s.ok()) {
        delete db;
	std::cout << "Could not finish writing to sst file: " << s.ToString() << std::endl;
        return -1;
    }

    // Ingest the external SST file
    IngestExternalFileOptions ingest_options;
    s = db->IngestExternalFile({tmp_file}, ingest_options);
    if (!s.ok()) {
	delete db;
	std::cout << "Could not ingest sst file: " << s.ToString() << std::endl;
	return -1;
    }
    
    // Clean up the temporary SST file
    std::remove(tmp_file.c_str());

    // Close the database
    delete db;

    return 0;
}

int runWorkload(DBEnv* env, const std::string& filename) {
  DB* db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options, &flush_options);

  auto compaction_listener = std::make_shared<CompactionsListener>();
  options.listeners.emplace_back(compaction_listener);

  options.memtable_whole_key_filtering = 1;
  options.optimize_filters_for_hits = 0;

  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return -1;
  }

  std::ifstream workload_file(filename);
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

  workload_file.open(filename);
  if (!workload_file.is_open()) {
    std::cerr << "Failed to reopen workload file." << std::endl;
    return -1;
  }

  auto it = db->NewIterator(read_options);
  uint32_t counter = 0;
  uint32_t q_not_found = 0;
  uint32_t q_ok = 0;

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
      if (s.IsNotFound()) {
        q_not_found++;
      } else if (s.ok()) {
	q_ok++;
      } else {
        std::cerr << s.ToString() << std::endl;
      }
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

  std::cout << "Number of NotFounds" << "\n" << q_not_found << std::endl;
  std::cout << "Number of oks" << "\n" << q_ok << std::endl;
  
  std::string db_get_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_CORE_JOULES);
  std::cout << "DB_GET_CORE_JOULES" << "\n" << db_get_core_joules << std::endl;

  std::string db_get_ret1_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_RET1_CORE_JOULES);
  std::cout << "DB_GET_RET1_CORE_JOULES" << "\n" << db_get_ret1_core_joules << std::endl;

  std::string db_get_filter_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_FILTER_CORE_JOULES);
  std::cout  << "DB_GET_FILTER_CORE_JOULES" << "\n" << db_get_filter_core_joules << std::endl;

  std::string db_get_index_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_INDEX_CORE_JOULES);
  std::cout << "DB_GET_INDEX_CORE_JOULES" << "\n" << db_get_index_core_joules << std::endl;

  std::string db_get_disk_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_DISK_CORE_JOULES);
  std::cout << "DB_GET_DISK_CORE_JOULES" << "\n" << db_get_disk_core_joules << std::endl;

  std::string db_get_ret2_core_joules = options.statistics->getHistogramString(rocksdb::Histograms::DB_GET_RET2_CORE_JOULES);
  std::cout << "DB_GET_RET2_CORE_JOULES" << "\n" << db_get_ret2_core_joules << std::endl;

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
