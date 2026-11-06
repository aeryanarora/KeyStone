include(FetchContent)

# Prefer a vendored copy (already cloned under deps/) to avoid a network
# fetch on every fresh clone/CI run; fall back to FetchContent otherwise.
if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_GOOGLETEST "${CMAKE_SOURCE_DIR}/third_party/googletest")
endif()

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
)
FetchContent_MakeAvailable(googletest)
