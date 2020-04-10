# Findbenchmark.cmake
# - Try to find benchmark
#
# The following variables are optionally searched for defaults
#  benchmark_ROOT_DIR:  Base directory where all benchmark components are found
#
# Once done this will define
#  GBENCHMARK_FOUND - System has benchmark
#  GBENCHMARK_INCLUDE_DIRS - The benchmark include directories
#  GBENCHMARK_LIBRARIES - The libraries needed to use benchmark

set(GBENCHMARK_ROOT_DIR "" CACHE PATH "Folder containing benchmark")

find_path(GBENCHMARK_INCLUDE_DIR "benchmark/benchmark.h"
  PATHS ${benchmark_ROOT_DIR}
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)
find_path(GBENCHMARK_INCLUDE_DIR "benchmark/benchmark.h")

find_library(GBENCHMARK_LIBRARY NAMES "benchmark"
  PATHS ${benchmark_ROOT_DIR}
  PATH_SUFFIXES lib lib64
  NO_DEFAULT_PATH)
find_library(GBENCHMARK_LIBRARY NAMES "benchmark")

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set GBENCHMARK_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(GBenchmark FOUND_VAR GBENCHMARK_FOUND
  REQUIRED_VARS GBENCHMARK_LIBRARY
  GBENCHMARK_INCLUDE_DIR)

if(GBENCHMARK_FOUND)
  set(GBENCHMARK_LIBRARIES ${GBENCHMARK_LIBRARY})
  set(GBENCHMARK_INCLUDE_DIRS ${GBENCHMARK_INCLUDE_DIR})
endif()

mark_as_advanced(GBENCHMARK_INCLUDE_DIR GBENCHMARK_LIBRARY)
