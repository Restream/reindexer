cmake_minimum_required(VERSION 3.0)
project(reindexer_samples)

# Configure cmake options
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
if (NOT MSVC)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wold-style-cast -fexceptions")
endif ()

find_package(PkgConfig REQUIRED) 

pkg_check_modules(REINDEXER_PKG REQUIRED libreindexer)

set (CPROTO_SAMPLE cproto_sample)
set (BUILTIN_SAMPLE builtin_sample)
add_executable(${CPROTO_SAMPLE} cproto_sample.cc)
add_executable(${BUILTIN_SAMPLE} builtin_sample.cc)

target_link_libraries(${CPROTO_SAMPLE} ${REINDEXER_PKG_LIBRARIES})
target_include_directories(${CPROTO_SAMPLE} PUBLIC ${REINDEXER_PKG_INCLUDE_DIRS})
target_compile_options(${CPROTO_SAMPLE} PUBLIC ${REINDEXER_PKG_CFLAGS_OTHER})

target_link_libraries(${BUILTIN_SAMPLE} ${REINDEXER_PKG_LIBRARIES})
target_include_directories(${BUILTIN_SAMPLE} PUBLIC ${REINDEXER_PKG_INCLUDE_DIRS})
target_compile_options(${BUILTIN_SAMPLE} PUBLIC ${REINDEXER_PKG_CFLAGS_OTHER})

