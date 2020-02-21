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

find_program (GTEST_PARALLEL "gtest-parallel")
if (GTEST_PARALLEL)
  add_test (NAME gtests COMMAND gtest-parallel --serialize_test_cases --print_test_times "./${TARGET}" )
else()
  add_test (NAME gtests COMMAND ${TARGET} --gtest_color=yes )
endif()

if (WITH_TSAN)
  set_tests_properties( gtests PROPERTIES ENVIRONMENT TSAN_OPTIONS=suppressions=${REINDEXER_SOURCE_PATH}/gtests/tsan.suppressions )
endif ()
