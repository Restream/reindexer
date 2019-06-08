cmake_minimum_required(VERSION 2.8)

project(tests)

SET(TARGET tests)
list(APPEND REINDEXER_LIBRARIES reindexer_server_library reindexer ${REINDEXER_LIBRARIES})

include_directories(fixtures mocks)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${REINDEXER_SOURCE_PATH})

file (GLOB_RECURSE SRCS *.cc *.h)

add_executable(${TARGET} ${SRCS})

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARY})
add_test (NAME gtests COMMAND ${TARGET} --gtest_color=yes )

if (WITH_TSAN)
  set_tests_properties( gtests PROPERTIES ENVIRONMENT TSAN_OPTIONS=suppressions=${REINDEXER_SOURCE_PATH}/gtests/tsan.suppressions )
endif ()
