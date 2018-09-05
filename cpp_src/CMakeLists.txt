cmake_minimum_required(VERSION 2.8)

# Configure cmake options
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(CMakeToolsHelpers OPTIONAL)
include(ExternalProject)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

option (WITH_ASAN "Enable AddressSanitized build" OFF)
option (WITH_TSAN "Enable ThreadSanitized build" OFF)
option (WITH_GPERF "Enable GPerfTools build" ON)
option (WITH_GCOV "Enable instrumented code coverage build" OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

enable_testing()
include(GNUInstallDirs)

project(reindexer)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/modules )

# Configure compile options
string( REPLACE "-DNDEBUG" "" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

if (MSVC)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -wd4244 -wd4267 -wd4996 -wd4717 -MP")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -wd4244 -wd4267 -wd4996 -wd4717 -wd4800 -wd4396 -wd4503 -MP")
else()
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra -Werror -Wold-style-cast")
endif ()

if (WITH_ASAN)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  add_definitions(-DREINDEX_WITH_ASAN)
elseif (WITH_TSAN)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
  add_definitions(-DREINDEX_WITH_TSAN)
endif ()

if (WITH_COVERAGE)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

# Build project
set(TARGET reindexer)

set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR} )

file (
  GLOB_RECURSE 
  SRCS 
    ${REINDEXER_SOURCE_PATH}/client/*
    ${REINDEXER_SOURCE_PATH}/core/*
    ${REINDEXER_SOURCE_PATH}/estl/*
    ${REINDEXER_SOURCE_PATH}/tools/*
    ${REINDEXER_SOURCE_PATH}/vendor/*
    ${REINDEXER_SOURCE_PATH}/debug/*
    ${REINDEXER_SOURCE_PATH}/net/*
)

include_directories(${REINDEXER_SOURCE_PATH})
include_directories(${REINDEXER_SOURCE_PATH}/vendor)

list(APPEND REINDEXER_LIBRARIES reindexer)
add_library(${TARGET} STATIC ${HDRS} ${SRCS} ${VENDORS})

## Dependencies

# tcmalloc
##########
if (NOT WITH_ASAN AND NOT WITH_TSAN AND WITH_GPERF)
  # tmalloc conflict with sanitizers, so disable it for sanitized builds
  find_package(Gperftools)
  if(GPERFTOOLS_TCMALLOC)
    include_directories(SYSTEM ${GPERFTOOLS_INCLUDE_DIR})
    add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)
    list (APPEND REINDEXER_LIBRARIES ${GPERFTOOLS_LIBRARIES})
  endif()
endif ()

# snappy
########
find_package(Snappy)
if (SNAPPY_FOUND)
  include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
  list(APPEND REINDEXER_LIBRARIES ${SNAPPY_LIBRARIES})
else ()
  message (STATUS "Snappy not found. Will download it")
  ExternalProject_Add(
    snappy_lib
    GIT_REPOSITORY "https://github.com/google/snappy.git"
    GIT_TAG "master"
    CMAKE_ARGS -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_CURRENT_BINARY_DIR}
    )
  include_directories (${CMAKE_CURRENT_BINARY_DIR}/include)
  link_directories(${CMAKE_CURRENT_BINARY_DIR})
  list(APPEND REINDEXER_LIBRARIES snappy)
endif ()

# leveldb
#########
if(GPERFTOOLS_TCMALLOC AND NOT WIN32)
  # shared version of leveldb can conflict with tcmalloc, so force static version of leveldb, if tcmalloc enabled
  set (LevelDB_NAMES libleveldb.a)
endif()
find_library(LevelDB_LIBRARY NAMES ${LevelDB_NAMES} leveldb HINTS $ENV{LEVELDB_ROOT}/lib)
find_path(LevelDB_INCLUDE_DIR NAMES leveldb/db.h HINTS $ENV{LEVELDB_ROOT}/include /opt/local/include /usr/local/include /usr/include)

if (NOT LevelDB_LIBRARY OR NOT LevelDB_INCLUDE_DIR)
  # Leveldb not found. Download it
  message (STATUS "LevelDB not found. Will download it")
  ExternalProject_Add(
    leveldb_lib
    GIT_REPOSITORY "https://github.com/restream/leveldb.git"
    GIT_TAG "master"
    CMAKE_ARGS -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF 
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
    -DCMAKE_CXX_FLAGS=-I${CMAKE_CURRENT_BINARY_DIR}/include
    -DCMAKE_EXE_LINKER_FLAGS=-L${CMAKE_CURRENT_BINARY_DIR}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_CURRENT_BINARY_DIR}
  )
  if (NOT SNAPPY_FOUND)
    add_dependencies(leveldb_lib snappy_lib)
  endif()
  link_directories(${CMAKE_CURRENT_BINARY_DIR})
  include_directories (${CMAKE_CURRENT_BINARY_DIR}/include)
  list(INSERT REINDEXER_LIBRARIES 1 leveldb)
  add_dependencies(reindexer leveldb_lib)
else ()
  message (STATUS "Found LevelDB: ${LevelDB_LIBRARY}")
  include_directories(SYSTEM ${LevelDB_INCLUDE_DIR})
  list(INSERT REINDEXER_LIBRARIES 1 ${LevelDB_LIBRARY})
endif ()

# System libraries
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED ON)
list(APPEND REINDEXER_LIBRARIES reindexer ${CMAKE_THREAD_LIBS_INIT} )

# librt
find_library(LIBRT rt)
if(LIBRT)
  list (APPEND REINDEXER_LIBRARIES ${LIBRT})
endif()

# execinfo
find_library(LIBEXECINFO execinfo)
if(LIBEXECINFO)
  list (APPEND REINDEXER_LIBRARIES ${LIBEXECINFO})
endif()

find_path(EXECINFO_INCLUDE_PATH execinfo.h)
if (EXECINFO_INCLUDE_PATH)
  add_definitions(-DREINDEX_WITH_EXECINFO=1)
endif()

if (WIN32)
  list(APPEND REINDEXER_LIBRARIES shlwapi dbghelp ws2_32)
endif ()

# Get version string
if(EXISTS ${PROJECT_SOURCE_DIR}/.git OR EXISTS ${PROJECT_SOURCE_DIR}/../.git )
  execute_process(WORKING_DIRECTORY ${REINDEXER_SOURCE_PATH} COMMAND git describe --tags OUTPUT_VARIABLE REINDEXER_VERSION_FULL OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

if (NOT REINDEXER_VERSION_FULL)
set (REINDEXER_VERSION_FULL "0.0.0-localbuild")
endif ()

file(WRITE ${PROJECT_BINARY_DIR}/reindexer_version.h "#pragma once\n#define REINDEX_VERSION \"${REINDEXER_VERSION_FULL}\"\n")
include_directories(${PROJECT_BINARY_DIR})

string ( REGEX REPLACE "(.*)([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)" "\\2.\\3.\\4" REINDEXER_VERSION ${REINDEXER_VERSION_FULL})

# Packaging and install stuff
if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/issue")
  file(READ "/etc/issue" LINUX_ISSUE)
endif ()
if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/os-release")
  file(READ "/etc/os-release" LINUX_ISSUE)
endif ()

set(CPACK_GENERATOR "TGZ")

if (WIN32) 
  set (CPACK_GENERATOR "NSIS")
elseif (LINUX_ISSUE MATCHES "Fedora" OR LINUX_ISSUE MATCHES "CentOS" OR LINUX_ISSUE MATCHES "Mandriva")
  set(CPACK_GENERATOR "RPM")
elseif (LINUX_ISSUE MATCHES "Ubuntu" OR LINUX_ISSUE MATCHES "Debian" OR LINUX_ISSUE MATCHES "Mint")
  set(CPACK_GENERATOR "DEB")
endif()

SET(CPACK_PACKAGE_VENDOR "Reindexer")
SET(CPACK_PACKAGE_CONTACT "Oleg Gerasimov <ogerasimov@gmail.com>")
SET(CPACK_PACKAGE_VERSION ${REINDEXER_VERSION})
set(CPACK_RESOURCE_FILE_LICENSE ${REINDEXER_SOURCE_PATH}/../LICENSE)

# Dependencies
set (CPACK_DEBIAN_PACKAGE_DEPENDS "")
set (CPACK_RPM_PACKAGE_REQUIRES "")

if (LevelDB_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libleveldb-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES},leveldb")
endif ()

if (SNAPPY_FOUND)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libsnappy-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES},snappy")
endif ()

if (GPERFTOOLS_TCMALLOC)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libgoogle-perftools4")
  SET(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES},gperftools-libs")
endif ()

# Remove first ',' from list of dependencies
if (NOT CPACK_DEBIAN_PACKAGE_DEPENDS STREQUAL "")
  string (SUBSTRING "${CPACK_DEBIAN_PACKAGE_DEPENDS}" 1 -1 CPACK_DEBIAN_PACKAGE_DEPENDS)
endif ()

if (NOT CPACK_RPM_PACKAGE_REQUIRES STREQUAL "")
  string (SUBSTRING "${CPACK_RPM_PACKAGE_REQUIRES}" 1 -1 CPACK_RPM_PACKAGE_REQUIRES)
endif ()

add_subdirectory(server)
add_subdirectory(cmd/reindexer_tool)
add_subdirectory(cmd/reindexer_server)
add_subdirectory(doc)

# Tests and benchmarks
find_package(GTest)
if (GTEST_FOUND)
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
    add_subdirectory(gtests/tests)
endif()

find_package(GBenchmark)
if (benchmark_FOUND)
    include_directories(SYSTEM ${benchmark_INCLUDE_DIR})
    add_subdirectory(gtests/bench)
endif()

add_custom_target(collect_coverage
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
  COMMAND lcov --directory . --capture -o coverage.info
  COMMAND lcov --remove coverage.info '/usr/*' '/Library/*' '${PROJECT_SOURCE_DIR}/vendor/*' -o coverage_filtered.info
  COMMAND genhtml coverage_filtered.info -o coverage_output
  COMMENT "Coollecting Reindexer coverage"
)
