# Each file below is its own GoogleTest binary so `ctest -R <name>` and
# parallel test execution (`ctest -j`) both work cleanly, and a crash in one
# suite doesn't take down unrelated tests.
set(KVDB_TEST_SOURCES
  test_coding.cc
  test_skiplist.cc
  test_memtable.cc
  test_wal.cc
  test_bloom.cc
  test_sstable.cc
  test_write_batch.cc
  test_db_basic.cc
  test_db_recovery.cc
  test_db_compaction.cc
  test_thread_pool.cc
  test_transaction.cc
  test_codec.cc
  test_tcp_transport.cc
  test_replication_log.cc
  test_replication_node.cc
  test_cluster_integration.cc
  test_fault_injection.cc
)

foreach(test_src ${KVDB_TEST_SOURCES})
  get_filename_component(test_name ${test_src} NAME_WE)
  add_executable(${test_name} ${test_src})
  target_link_libraries(${test_name} PRIVATE kvdb_core GTest::gtest GTest::gtest_main GTest::gmock)
  target_include_directories(${test_name} PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${test_name} COMMAND ${test_name})
  set_tests_properties(${test_name} PROPERTIES TIMEOUT 120)
endforeach()
