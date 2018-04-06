cmake_minimum_required(VERSION 2.8)

project(benchmarking)

set(TARGET benchmarking)
set(FT_TARGET ft_benchmarking)

include_directories(fixtures)
include_directories(tools)
include_directories(${benchmark_INCLUDE_DIRS})

file (GLOB_RECURSE FIXT_SRCS fixtures/*)
file (GLOB_RECURSE FT_FIXT_SRCS fixtures/ft_* fixtures/base_fixture.*)
file (GLOB_RECURSE TOOLS_SRCS tools/*)

add_executable(${TARGET} ${FIXT_SRCS} ${TOOLS_SRCS} reindexer_bench.cc)
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${benchmark_LIBRARY})

add_executable(${FT_TARGET} ${FT_FIXT_SRCS} ${TOOLS_SRCS} ft_bench.cc)
target_link_libraries(${FT_TARGET} ${REINDEXER_LIBRARIES} ${benchmark_LIBRARY})

add_test (NAME bench COMMAND ${TARGET} --benchmark_color=true --benchmark_counters_tabular=true)
add_test (NAME ft_bench COMMAND ${FT_TARGET} --benchmark_color=true --benchmark_counters_tabular=true WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
