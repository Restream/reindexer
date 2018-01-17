cmake_minimum_required(VERSION 2.8)

project(reindexer_server)

set(TARGET reindexer_server)


file(
    GLOB_RECURSE
    SRCS
    ${PROJECT_SOURCE_DIR}/*.cc
        ${REINDEXER_SOURCE_PATH}/pprof/*
)

add_executable(${TARGET} ${SRCS})

include_directories(${PROJECT_SOURCE_DIR})

target_link_libraries(
    ${TARGET}
        reindexer
        ${LevelDB_LIBRARY}
        ${SNAPPY_LIBRARIES}
        ${GPERFTOOLS_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
)
