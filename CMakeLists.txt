cmake_minimum_required(VERSION 3.20)
project(distributed_kv LANGUAGES CXX VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(KVDB_BUILD_TESTS     "Build unit and integration tests" ON)
option(KVDB_BUILD_BENCHMARKS "Build Google Benchmark microbenchmarks" ON)
option(KVDB_ENABLE_GRPC     "Build the gRPC transport (requires gRPC + protobuf)" OFF)
option(KVDB_ENABLE_ASAN     "Build with AddressSanitizer" OFF)
option(KVDB_ENABLE_TSAN     "Build with ThreadSanitizer" OFF)

# -fno-omit-frame-pointer keeps `perf record -g` call graphs usable in optimised
# builds, which is the whole point of the profiling workflow in docs/.
add_compile_options(-Wall -Wextra -Wno-unused-parameter -fno-omit-frame-pointer)

if(KVDB_ENABLE_ASAN)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address)
endif()
if(KVDB_ENABLE_TSAN)
  add_compile_options(-fsanitize=thread -g)
  add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Core library: storage engine, concurrency primitives, replication, transport.
# ---------------------------------------------------------------------------
set(KVDB_CORE_SOURCES
  src/common/crc32c.cc
  src/common/logger.cc
  src/common/file_util.cc
  src/storage/memtable.cc
  src/storage/write_batch.cc
  src/storage/wal.cc
  src/storage/bloom.cc
  src/storage/sstable.cc
  src/storage/db.cc
  src/storage/db_compaction.cc
  src/concurrency/thread_pool.cc
  src/concurrency/transaction.cc
  src/net/codec.cc
  src/net/event_loop.cc
  src/net/tcp_server.cc
  src/net/tcp_client.cc
  src/replication/replication_log.cc
  src/replication/replication_transport.cc
  src/replication/replication_node.cc
  src/server/kv_server.cc
  src/client/kv_client.cc
)

add_library(kvdb_core STATIC ${KVDB_CORE_SOURCES})
target_include_directories(kvdb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kvdb_core PUBLIC Threads::Threads)

# ---------------------------------------------------------------------------
# Optional gRPC transport.
# ---------------------------------------------------------------------------
if(KVDB_ENABLE_GRPC)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Grpc.cmake)
endif()

# ---------------------------------------------------------------------------
# Binaries
# ---------------------------------------------------------------------------
add_executable(kvdb-server src/server/main.cc)
target_link_libraries(kvdb-server PRIVATE kvdb_core)

add_executable(kvdb-cli src/client/main.cc)
target_link_libraries(kvdb-cli PRIVATE kvdb_core)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(KVDB_BUILD_TESTS)
  enable_testing()
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/GoogleTest.cmake)
  add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(KVDB_BUILD_BENCHMARKS)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/GoogleBenchmark.cmake)
  add_subdirectory(bench)
endif()

message(STATUS "distributed-kv configuration:")
message(STATUS "  build type   : ${CMAKE_BUILD_TYPE}")
message(STATUS "  tests        : ${KVDB_BUILD_TESTS}")
message(STATUS "  benchmarks   : ${KVDB_BUILD_BENCHMARKS}")
message(STATUS "  gRPC transport: ${KVDB_ENABLE_GRPC}")
