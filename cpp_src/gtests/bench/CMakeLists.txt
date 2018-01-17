cmake_minimum_required(VERSION 2.8)

project(benchmarking)

set(TARGET benchmarking)

include_directories(fixtures)
include_directories(${benchmark_INCLUDE_DIRS})

add_executable(${TARGET} reindexer_bench.cc fixtures/reindexer_fixture.cc)

list(APPEND
    LIBRARY_LIST
        ${GPERFTOOLS_TCMALLOC}
        ${GPERFTOOLS_PROFILER}
        "reindexer"
        ${benchmark_LIBRARY}
        ${LevelDB_LIBRARY}
        ${SNAPPY_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
)

find_library(LIBRT rt)
if(LIBRT)
    list(APPEND LIBRARY_LIST "${LIBRT}")
endif()

target_link_libraries(${TARGET} ${LIBRARY_LIST})
