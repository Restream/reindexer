project(reindexer)
cmake_minimum_required(VERSION 2.8)
enable_testing()
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR}/cpp_src)
add_subdirectory(cpp_src)
option (WITH_PYTHON "Enable build python connector" ON)

add_subdirectory(connectors)

find_program (BASH_CMD bash)
if (BASH_CMD)
  add_test (rx_tool_test ${BASH_CMD}
      ${CMAKE_CURRENT_SOURCE_DIR}/cpp_src/cmd/reindexer_tool/contrib/rx_tool_tests.sh
      ${CMAKE_CURRENT_SOURCE_DIR}/cpp_src/cmd/reindexer_tool/contrib/dbtest.dump
      /tmp/reindexer_test
      ${CMAKE_CURRENT_SOURCE_DIR})
endif (BASH_CMD)
