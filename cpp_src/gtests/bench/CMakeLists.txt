cmake_minimum_required(VERSION 3.18)

project(benchmarking)

set(TARGET benchmarking)
set(FT_TARGET ft_benchmarking)
set(KNN_TARGET knn_benchmarking)
set(RX_BENCH_LIB reindexer_bench_lib)

option(BENCH_REPORT "Enable CI benchmarks report" OFF)

include_directories(fixtures)
include_directories(tools)

file (GLOB_RECURSE FIXT_SRCS fixtures/*)
file (GLOB_RECURSE FT_FIXT_SRCS fixtures/ft_* fixtures/base_fixture.*)
file (GLOB_RECURSE KNN_FIXT_SRCS fixtures/knn_* fixtures/base_fixture.* fixtures/ft_base.*)
file (GLOB_RECURSE TOOLS_SRCS tools/*)

add_library(${RX_BENCH_LIB} OBJECT ${TOOLS_SRCS} ${FIXT_SRCS})
target_compile_definitions(${RX_BENCH_LIB} PRIVATE RX_BENCH_DICT_PATH="${CMAKE_CURRENT_SOURCE_DIR}/dict.txt")

add_executable(${TARGET} reindexer_bench.cc)
target_link_libraries(${TARGET} ${RX_BENCH_LIB} ${REINDEXER_LIBRARIES} ${GBENCHMARK_LIBRARY})

add_executable(${FT_TARGET} ft_bench.cc)
target_link_libraries(${FT_TARGET} ${RX_BENCH_LIB} ${REINDEXER_LIBRARIES} ${GBENCHMARK_LIBRARY})

add_executable(${KNN_TARGET} knn_bench.cc)
target_link_libraries(${KNN_TARGET} ${RX_BENCH_LIB} ${REINDEXER_LIBRARIES} ${GBENCHMARK_LIBRARY})

# Enable export to provide readable stacktraces
set_property(TARGET ${TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${TARGET} PROPERTY DEFINE_SYMBOL "")
set_property(TARGET ${FT_TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${FT_TARGET} PROPERTY DEFINE_SYMBOL "")
set_property(TARGET ${KNN_TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${KNN_TARGET} PROPERTY DEFINE_SYMBOL "")

if(ENABLE_PCH)
  set(PREC_HDRS
    ${REINDEXER_SOURCE_PATH}/core/reindexer.h
    ${REINDEXER_SOURCE_PATH}/core/query/query.h
    ${REINDEXER_SOURCE_PATH}/core/queryresults/queryresults.h
    ${REINDEXER_SOURCE_PATH}/core/item.h

    fixtures/base_fixture.h

    tools/allocs_tracker.h
    tools/helpers.h
    tools/reporter.h
    tools/sequence.h
  )

  target_precompile_headers(${RX_BENCH_LIB} PRIVATE ${PREC_HDRS})
  target_precompile_headers(${TARGET} REUSE_FROM ${RX_BENCH_LIB})
  target_precompile_headers(${FT_TARGET} REUSE_FROM ${TARGET})
  target_precompile_headers(${KNN_TARGET} REUSE_FROM ${TARGET})
endif()

if(BENCH_REPORT)
    message("Benchmark report flag is activated")
    message("Run benchmarks manually")
else()
    add_test (NAME bench COMMAND ${TARGET} --benchmark_color=true --benchmark_counters_tabular=true --benchmark_min_time=0.1)
    add_test (NAME ft_bench COMMAND ${FT_TARGET} --benchmark_color=true --benchmark_counters_tabular=true --benchmark_min_time=0.1)
    add_test (NAME knn_bench COMMAND ${KNN_TARGET} --benchmark_color=true --benchmark_counters_tabular=true --benchmark_min_time=0.1)
endif()
