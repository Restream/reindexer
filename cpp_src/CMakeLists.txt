cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(CMakeToolsHelpers OPTIONAL)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

enable_testing()

include(GNUInstallDirs)

project(reindexer)

string( REPLACE "-DNDEBUG" "" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra -Werror -Wold-style-cast")

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/modules )

if (WITH_ASAN)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  add_definitions(-DREINDEX_WITH_ASAN)
elseif (WITH_TSAN)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
  add_definitions(-DREINDEX_WITH_TSAN)
else ()
  find_package(Gperftools)
endif ()

if (WITH_COVERAGE)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()


add_custom_target(collect_coverage
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
  COMMAND lcov --directory . --capture -o coverage.info
  COMMAND lcov --remove coverage.info '/usr/*' '/Library/*' '${PROJECT_SOURCE_DIR}/vendor/*' -o coverage_filtered.info
  COMMAND genhtml coverage_filtered.info -o coverage_output
  COMMENT "Coollecting Reindexer coverage"
)


set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED ON)
find_library(LevelDB_LIBRARY NAMES leveldb)
find_package(Snappy REQUIRED ON)
find_package(GTest)
find_package(GBenchmark)
find_library(LIBRT rt)
find_library(LIBEXECINFO execinfo)
find_path(EXECINFO_INCLUDE_PATH execinfo.h)

set(TARGET reindexer)

set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR} )

set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

file (
  GLOB_RECURSE 
  SRCS 
    ${REINDEXER_SOURCE_PATH}/core/*
    ${REINDEXER_SOURCE_PATH}/estl/*
    ${REINDEXER_SOURCE_PATH}/tools/*
    ${REINDEXER_SOURCE_PATH}/vendor/*
    ${REINDEXER_SOURCE_PATH}/debug/*
    ${REINDEXER_SOURCE_PATH}/net/*
)

include_directories(${REINDEXER_SOURCE_PATH})
include_directories(${REINDEXER_SOURCE_PATH}/vendor)
add_library(${TARGET} STATIC ${HDRS} ${SRCS} ${VENDORS})

list(APPEND REINDEXER_LIBRARIES
    reindexer
    ${LevelDB_LIBRARY}
    ${SNAPPY_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
)

if(GPERFTOOLS_TCMALLOC)
  add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)
  list (APPEND REINDEXER_LIBRARIES ${GPERFTOOLS_LIBRARIES})
endif()
if(LIBRT)
  list (APPEND REINDEXER_LIBRARIES ${LIBRT})
endif()
if(LIBEXECINFO)
  list (APPEND REINDEXER_LIBRARIES ${LIBEXECINFO})
endif()
if (EXECINFO_INCLUDE_PATH)
  add_definitions(-DREINDEX_WITH_EXECINFO=1)
endif()

if (GTEST_FOUND)
    add_subdirectory(gtests/tests)
    add_subdirectory(gtests/memory)
endif()

if (benchmark_FOUND)
    add_subdirectory(gtests/bench)
endif()

if(EXISTS ${PROJECT_SOURCE_DIR}/.git OR EXISTS ${PROJECT_SOURCE_DIR}/../.git )
  execute_process(WORKING_DIRECTORY ${REINDEXER_SOURCE_PATH} COMMAND git describe --tags OUTPUT_VARIABLE REINDEXER_VERSION_FULL OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

if (NOT REINDEXER_VERSION_FULL)
set (REINDEXER_VERSION_FULL "0.0.0-localbuild")
endif ()

string ( REGEX REPLACE "(.*)([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)" "\\2.\\3.\\4" REINDEXER_VERSION ${REINDEXER_VERSION_FULL})
add_definitions(-DREINDEX_VERSION=${REINDEXER_VERSION_FULL})

if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/issue")
  file(READ "/etc/issue" LINUX_ISSUE)
endif ()
if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/os-release")
  file(READ "/etc/os-release" LINUX_ISSUE)
endif ()

set(CPACK_GENERATOR "TGZ")
if(LINUX_ISSUE MATCHES "Fedora" OR LINUX_ISSUE MATCHES "CentOS" OR LINUX_ISSUE MATCHES "Mandriva")
  set(CPACK_GENERATOR "RPM")
endif()
if(LINUX_ISSUE MATCHES "Ubuntu" OR LINUX_ISSUE MATCHES "Debian" OR LINUX_ISSUE MATCHES "Mint")
  set(CPACK_GENERATOR "DEB")
endif()

SET(CPACK_PACKAGE_VENDOR "Reindexer")
SET(CPACK_PACKAGE_CONTACT "Oleg Gerasimov <ogerasimov@gmail.com>")
SET(CPACK_PACKAGE_VERSION ${REINDEXER_VERSION})
SET(CPACK_DEBIAN_PACKAGE_DEPENDS "libleveldb-dev")

add_subdirectory(cmd/reindexer_tool)
add_subdirectory(cmd/reindexer_server)

add_subdirectory(doc)

# install(TARGETS reindexer
#     RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
#     LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
#     ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
# )
