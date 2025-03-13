cmake_minimum_required(VERSION 3.18)

project(reindexer)

enable_testing()
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_PCH_WARN_INVALID OFF)
set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR}/cpp_src)

add_subdirectory(cpp_src)
