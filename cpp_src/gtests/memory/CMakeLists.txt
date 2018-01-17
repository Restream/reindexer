cmake_minimum_required(VERSION 2.8)

project(memory_test)

SET(TARGET memory_test)
add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)

include_directories(fixtures)
include_directories(../tests/fixtures)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${REINDEXER_SOURCE_PATH})
add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)

file (
        GLOB_RECURSE
        SRCS
        *.cc
        *.h
)
add_executable(${TARGET} ${SRCS})

target_link_libraries(
    ${TARGET} 
        ${GTEST_LIBRARY}
        reindexer
        ${LevelDB_LIBRARY}
        ${SNAPPY_LIBRARIES}
        ${GPERFTOOLS_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
)

add_custom_command(TARGET ${TARGET} POST_BUILD
  COMMAND "${CMAKE_COMMAND}" -E copy
     ${CMAKE_CURRENT_SOURCE_DIR}"/dict"
     ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"/dict"
  COMMENT "Copying to output directory")
