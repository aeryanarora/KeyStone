include(FetchContent)

# Prefer a vendored copy (already cloned under third_party/) to avoid a
# network fetch on every fresh clone/CI run; fall back to FetchContent
# otherwise. The override variable name must match the FetchContent_Declare
# name below, uppercased: FETCHCONTENT_SOURCE_DIR_GOOGLEBENCHMARK for a
# target named "googlebenchmark" -- not FETCHCONTENT_SOURCE_DIR_BENCHMARK,
# which CMake silently ignores, falling through to a git clone instead.
if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/benchmark/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_GOOGLEBENCHMARK "${CMAKE_SOURCE_DIR}/third_party/benchmark")
endif()

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.9.1
)
FetchContent_MakeAvailable(googlebenchmark)
