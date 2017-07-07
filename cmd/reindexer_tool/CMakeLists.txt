cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
project(reindexer_dump)

find_library(LevelDB_LIBRARY NAMES leveldb)
find_package (Threads)

set(SOURCE_PATH ../core)

set(SOURCE_EXE main.cpp)
file(GLOB ALL_SOURCES
	"../../core/*.cc"
	"../../cbinding/*.cc"
	"../../tools/*.cc"
	"../../query/*.cc"
	"../../dsl/*.cc"
	"../../pprof/backtrace.cc"
	"../../vendor/*/*.cc"
	"../../vendor/cxxopts/cxxopts.hpp"
	"*.cpp"
	"../../init/*.cc"
	"../../algoritm/full_text_search/*.cc"
	"../../algoritm/full_text_search/dataholder/*.cc"
	"../../algoritm/full_text_search/dataholder/*.h"
	"../../algoritm/full_text_search/searchers/*.cc"
	"../../algoritm/full_text_search/searchers/*.h"
	"../../algoritm/full_text_search/searchers/base_searcher/*.cc"
	"../../algoritm/full_text_search/tool/*.cc"
	"../../algoritm/full_text_search/merger/*.cc"
)

include_directories(../../vendor ../.. ../../tools .)

file(
	GLOB MAIN
	${ALL_SOURCES}
	reindexer_tool.cc
)

add_executable(main ${MAIN})
target_link_libraries(main ${CMAKE_THREAD_LIBS_INIT} ${LevelDB_LIBRARY})