cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(CMakeToolsHelpers OPTIONAL)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()
# set (CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} "-Wl,-no_pie")
# set (CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-Wall -Wextra -Werror")
# set (CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++11 -Wall -Wextra -Werror -Wold-style-cast")

project(reindexer)

set (CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++11")

set(
  CMAKE_MODULE_PATH
    ${CMAKE_MODULE_PATH}
    ${PROJECT_SOURCE_DIR}/cmake/modules
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED ON)
find_library(LevelDB_LIBRARY NAMES leveldb)
find_package(Gperftools)
find_package(Snappy REQUIRED ON)
find_package(GTest)
find_package(GBenchmark)

set(
  CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/.build
  CACHE
    INTERNAL "Directory where all .lib|.a files are dumped."
)

set(
  CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/.build
  CACHE
    PATH "Directory where all .so files are dumped."
)

set(
  CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/.build
  CACHE
    PATH "Directory where all executable files are dumped."
)

set(
  CMAKE_DOC_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/.build
  CACHE
    PATH "Directory where all documentation files are dumped."
)

set(TARGET reindexer)

set(
  REINDEXER_SOURCE_PATH
    ${PROJECT_SOURCE_DIR}
)

file (
  GLOB_RECURSE 
  SRCS 
    ${REINDEXER_SOURCE_PATH}/core/*
    ${REINDEXER_SOURCE_PATH}/estl/*
    ${REINDEXER_SOURCE_PATH}/tools/*
    ${REINDEXER_SOURCE_PATH}/vendor/*
    ${REINDEXER_SOURCE_PATH}/debug/*
)

include_directories(${REINDEXER_SOURCE_PATH})
include_directories(${REINDEXER_SOURCE_PATH}/vendor)
add_library(${TARGET} STATIC ${HDRS} ${SRCS} ${VENDORS})

if(GPERFTOOLS_TCMALLOC)
  add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)
else()
    message(WARNING "Google GPERFTOOLS_TCMALLOC")
endif()

if (GTEST_FOUND)
    add_subdirectory(gtests/tests)
    add_subdirectory(gtests/memory)
else()
    message(WARNING "Google Test Framework NOT FOUND. Testing is not available.")
endif()

if (benchmark_FOUND)
    add_subdirectory(gtests/bench)
else()
    message(WARNING "Google Benchmark Framework NOT FOUND. Benchmarking is not available.")
endif()

add_subdirectory(cmd/reindexer_tool)
add_subdirectory(cmd/reindexer_server)

add_subdirectory(doc)
