cmake_minimum_required(VERSION 3.18)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(TARGET custom-tidy-checks)
project(${TARGET})

find_package(LLVM REQUIRED CONFIG)
find_package(Clang REQUIRED CONFIG)

message(STATUS "LLVM found: ${LLVM_DIR}")
message(STATUS "Clang found: ${Clang_DIR}")

add_definitions(${LLVM_DEFINITIONS})
include_directories(${LLVM_INCLUDE_DIRS})
include_directories(${CLANG_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/src)

file (
  GLOB_RECURSE
  SRCS
  ${PROJECT_SOURCE_DIR}/src/*
)

add_library(${TARGET} MODULE ${SRCS})

target_link_libraries(${TARGET}
    clangTidy
    clangTidyMain
    clangASTMatchers
    clangAST
    clangBasic
    LLVMSupport
)

set_target_properties(${TARGET} PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)
