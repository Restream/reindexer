cmake_minimum_required(VERSION 2.8)

project(benchmarking)

set(TARGET benchmarking)

include_directories(fixtures)
include_directories(tools)
include_directories(${benchmark_INCLUDE_DIRS})

file (GLOB_RECURSE SRCS *.cc *.h)

add_executable(${TARGET} ${SRCS})
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${benchmark_LIBRARY})
add_test (NAME bench COMMAND ${TARGET} --benchmark_color=true --benchmark_counters_tabular=true)
