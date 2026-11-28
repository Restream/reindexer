cmake_minimum_required(VERSION 3.18)

find_program(CCACHE_PROGRAM ccache)

if(CCACHE_PROGRAM)
    message(STATUS "ccache found: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER   ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CUDA_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
else()
    message(STATUS "ccache NOT found. Standard compilation will be used.")
endif()

project(reindexer)

find_program(MOLD_PROGRAM mold)

if(MOLD_PROGRAM)
    message(STATUS "mold found: ${MOLD_PROGRAM}")
    add_link_options(-fuse-ld=mold)
else()
    message(STATUS "mold NOT found. Default linker will be used.")
endif()

enable_testing()
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR}/cpp_src)
# Do not scan for C++ modules to save some overhead
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

add_subdirectory(cpp_src)

# uninstall target
if(NOT TARGET uninstall)
  configure_file(
    "${PROJECT_SOURCE_DIR}/cpp_src/cmake/cmake_uninstall.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

  add_custom_target(uninstall COMMAND ${CMAKE_COMMAND} -P ${PROJECT_BINARY_DIR}/cmake_uninstall.cmake)
endif()
