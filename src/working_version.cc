/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include <parse_arguments.h>
#include <run_workload.h>
#include <db_env.h>

int main(int argc, char *argv[]) {
  // check db_env.h for the contents of DBEnv and also 
  // the definitions of the singleton experimental environment
  DBEnv *env = DBEnv::GetInstance();

  Options options;
  // parse the command line arguments
  if (parse_arguments(argc, argv, env)) {
    exit(1);
  }

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(kDBPath, options);
    std::cout << "Destroying database ..." << std::endl;
  }

  if (env->clear_system_cache) {
    std::cout << "Clearing system cache ..." << std::endl;
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }

  //BulkLoad("workload.txt", kDBPath);

  auto start_bulk_load = std::chrono::high_resolution_clock::now();
  runWorkload(env, "workload.txt");
  auto end_bulk_load = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> bulk_load_duration = end_bulk_load - start_bulk_load;
  std::cout << "Bulk load time: " << bulk_load_duration.count() << " seconds\n";

  printLSM();

  auto start_query = std::chrono::high_resolution_clock::now();
  runWorkload(env, "query_workload.txt");
  auto end_query = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> query_duration = end_query - start_query;
  std::cout << "Query time: " << query_duration.count() << " seconds\n";
  return 0;
}
