cmake_minimum_required(VERSION 2.8)

project(benchmarking)

set(TARGET benchmarking)
set(FT_TARGET ft_benchmarking)

include_directories(fixtures)
include_directories(tools)
include_directories(${GBENCHMARK_INCLUDE_DIRS})

file (GLOB_RECURSE FIXT_SRCS fixtures/*)
file (GLOB_RECURSE FT_FIXT_SRCS fixtures/ft_* fixtures/base_fixture.*)
file (GLOB_RECURSE TOOLS_SRCS tools/*)

set (BENCH_DICT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/dict.txt)

add_executable(${TARGET} ${FIXT_SRCS} ${TOOLS_SRCS} reindexer_bench.cc)
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GBENCHMARK_LIBRARY})
target_compile_definitions(${TARGET} PRIVATE -DRX_BENCH_DICT_PATH="${BENCH_DICT_PATH}")

add_executable(${FT_TARGET} ${FT_FIXT_SRCS} ${TOOLS_SRCS} ft_bench.cc)
target_link_libraries(${FT_TARGET} ${REINDEXER_LIBRARIES} ${GBENCHMARK_LIBRARY})
target_compile_definitions(${FT_TARGET} PRIVATE -DRX_BENCH_DICT_PATH="${BENCH_DICT_PATH}")

add_test (NAME bench COMMAND ${TARGET} --benchmark_color=true --benchmark_counters_tabular=true --benchmark_min_time=0.1)
add_test (NAME ft_bench COMMAND ${FT_TARGET} --benchmark_color=true --benchmark_counters_tabular=true --benchmark_min_time=0.1)
