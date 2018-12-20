cmake_minimum_required(VERSION 2.8)
enable_testing()
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR}/cpp_src)
add_subdirectory(cpp_src)
option (WITH_PYTHON "Enable build python connector" ON)

add_subdirectory(connectors)
