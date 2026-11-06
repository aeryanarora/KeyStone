add_executable(kvdb-bench-cluster bench_cluster.cc)
target_link_libraries(kvdb-bench-cluster PRIVATE kvdb_core)

add_executable(kvdb-bench-storage bench_storage.cc)
target_link_libraries(kvdb-bench-storage PRIVATE kvdb_core benchmark::benchmark)
