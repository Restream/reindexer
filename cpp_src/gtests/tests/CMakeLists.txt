cmake_minimum_required(VERSION 2.8)

project(tests)

SET(TARGET tests)

include_directories(fixtures)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${REINDEXER_SOURCE_PATH})

file (GLOB_RECURSE SRCS *.cc *.h)

add_executable(${TARGET} ${SRCS})
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARY})
add_test (NAME gtests COMMAND ${TARGET} --gtest_color=yes)
