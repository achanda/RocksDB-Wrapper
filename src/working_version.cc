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
  BulkLoad("workload.txt");
  int s = runWorkload(env);
  return 0;
}
