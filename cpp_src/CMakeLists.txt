cmake_minimum_required(VERSION 3.10)

# Configure cmake options
if (MSVC)
  # Enable C++20 for windows build to be able to use designated initializers.
  # GCC/Clang support them even with C++17.
  set(CMAKE_CXX_STANDARD 20)
else ()
  set(CMAKE_CXX_STANDARD 17)
endif ()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(CMakeToolsHelpers OPTIONAL)
include(ExternalProject)
include(ProcessorCount)

set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

option (WITH_ASAN "Enable AddressSanitized build" OFF)
option (WITH_TSAN "Enable ThreadSanitized build" OFF)
option (WITH_GCOV "Enable instrumented code coverage build" OFF)
option (WITH_STDLIB_DEBUG "Enable compiler's debug flags for stdlib behaviour validation (gcc/clang)" OFF)

if (WIN32)
  option (WITH_CPPTRACE "Enable CppTrace" ON)
endif()

option (ENABLE_LIBUNWIND "Enable libunwind" ON)
option (ENABLE_TCMALLOC "Enable tcmalloc extensions" ON)
option (ENABLE_JEMALLOC "Enable jemalloc extensions" ON)
option (ENABLE_ROCKSDB "Enable rocksdb storage" ON)
option (ENABLE_GRPC "Enable GRPC service" OFF)
option (ENABLE_SSE "Enable SSE instructions" ON)
option (ENABLE_SERVER_AS_PROCESS_IN_TEST "Enable server as process" OFF)


if (NOT GRPC_PACKAGE_PROVIDER)
   set (GRPC_PACKAGE_PROVIDER "CONFIG")
endif ()

if (WIN32)
  option (LINK_RESOURCES "Link web resources as binary data" OFF)
else()
  option (LINK_RESOURCES "Link web resources as binary data" ON)
endif()

set (REINDEXER_VERSION_DEFAULT "3.24.0")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

enable_testing()
include(GNUInstallDirs)

project(reindexer)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/modules )
include (CheckLinkerFlag)

include (TargetArch)
target_architecture(COMPILER_TARGET_ARCH)

# Configure compile options
if (MSVC)
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -Zi")
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O2 -Zi")
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -Zi")
  set(CMAKE_C_FLAGS_RELEASE "-O2 -DNDEBUG -Zi")
else ()
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g1")
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O3 -g1")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
  set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")
endif ()
if (${COMPILER_TARGET_ARCH} STREQUAL "e2k")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g0")
  add_definitions(-D__E2K__)
  add_definitions(-D__LCC__)
endif ()

if (NOT MSVC AND NOT APPLE)
  check_linker_flag (-gz cxx_linker_supports_gz)
  if (cxx_linker_supports_gz) 
    set (CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -gz")
  endif ()
endif ()

if (MSVC)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -wd4244 -wd4267 -wd4996 -wd4717 -MP")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -wd4244 -wd4267 -wd4996 -wd4717 -wd4800 -wd4396 -wd4503 -MP")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -SAFESEH:NO")
else()
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -Wswitch-enum")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wextra -Werror -Wswitch-enum -Wold-style-cast -fexceptions")
  if (${COMPILER_TARGET_ARCH} STREQUAL "e2k")
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -gline -fverbose-asm")
    set (CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-parameter")
  endif()
endif ()

set (EXTRA_FLAGS "")

if (WITH_ASAN AND WITH_TSAN)
  message(FATAL_ERROR "You cannot use the ASAN and TSAN options at the same time, CMake will exit.")
endif()

if (WITH_ASAN)
  set (EXTRA_FLAGS "-fsanitize=address")
  add_definitions(-DREINDEX_WITH_ASAN)
elseif (WITH_TSAN)
  set (EXTRA_FLAGS "-fsanitize=thread")
  add_definitions(-DREINDEX_WITH_TSAN)
endif ()
if (WITH_GCOV)
  set (EXTRA_FLAGS "-fprofile-arcs -ftest-coverage")
endif ()

if (WITH_STDLIB_DEBUG)
  add_definitions(-DRX_WITH_STDLIB_DEBUG=1)
  if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_definitions(-D_GLIBCXX_DEBUG)
    add_definitions(-D_GLIBCXX_DEBUG_PEDANTIC)
  else ()
    message ("Option 'WITH_STDLIB_DEBUG' was requested, but there is not such option for current toolcain: '${CMAKE_CXX_COMPILER_ID}'. Disabling...")
  endif ()
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXTRA_FLAGS}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${EXTRA_FLAGS}")

# Build project
set(TARGET reindexer)

set(REINDEXER_SOURCE_PATH ${PROJECT_SOURCE_DIR})
set(REINDEXER_BINARY_PATH ${PROJECT_BINARY_DIR})


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
  ${REINDEXER_SOURCE_PATH}/replicator/*
  ${REINDEXER_SOURCE_PATH}/coroutine/*
)

string(REGEX REPLACE "([][+*()^])" "\\\\\\1" BASE_CORO_CONTEXT_DIR "${REINDEXER_SOURCE_PATH}/vendor/koishi")
set (CONTEXT_ASM_DIR "${BASE_CORO_CONTEXT_DIR}/fcontext/asm")
list(FILTER SRCS EXCLUDE REGEX "${BASE_CORO_CONTEXT_DIR}/.*" )

if ( UNIX )
  enable_language(ASM)
  if (APPLE)
    if ( ${COMPILER_TARGET_ARCH} STREQUAL "arm" )
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_arm_aapcs_macho_gas.S
            ${CONTEXT_ASM_DIR}/make_arm_aapcs_macho_gas.S
            )
    elseif  ( ${COMPILER_TARGET_ARCH} STREQUAL "arm64" OR ${COMPILER_TARGET_ARCH} STREQUAL "aarch64" )
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_arm64_aapcs_macho_gas.S
            ${CONTEXT_ASM_DIR}/make_arm64_aapcs_macho_gas.S
            )
    elseif  ( ${COMPILER_TARGET_ARCH} STREQUAL "x86_64")
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_x86_64_sysv_macho_gas.S
            ${CONTEXT_ASM_DIR}/make_x86_64_sysv_macho_gas.S
            )
    elseif ( ${COMPILER_TARGET_ARCH} STREQUAL "i386")
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_i386_sysv_macho_gas.S
            ${CONTEXT_ASM_DIR}/make_i386_sysv_macho_gas.S
            )
    else ()
        message (FATAL_ERROR "Unsupported APPLE-platform architecture: ${COMPILER_TARGET_ARCH}. Unable to chose context sources")
    endif ()
  else ()
    if ( ${COMPILER_TARGET_ARCH} STREQUAL "arm" )
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_arm_aapcs_elf_gas.S
            ${CONTEXT_ASM_DIR}/make_arm_aapcs_elf_gas.S
            )
    elseif  ( ${COMPILER_TARGET_ARCH} STREQUAL "arm64" OR ${COMPILER_TARGET_ARCH} STREQUAL "aarch64" )
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_arm64_aapcs_elf_gas.S
            ${CONTEXT_ASM_DIR}/make_arm64_aapcs_elf_gas.S
            )
    elseif  ( ${COMPILER_TARGET_ARCH} STREQUAL "x86_64")
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_x86_64_sysv_elf_gas.S
            ${CONTEXT_ASM_DIR}/make_x86_64_sysv_elf_gas.S
            )
    elseif ( ${COMPILER_TARGET_ARCH} STREQUAL "i386")
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_i386_sysv_elf_gas.S
            ${CONTEXT_ASM_DIR}/make_i386_sysv_elf_gas.S
            )
    elseif (NOT ${COMPILER_TARGET_ARCH} STREQUAL "e2k")
        message (FATAL_ERROR "Unsupported Linux-platform architecture: ${COMPILER_TARGET_ARCH}. Unable to chose context sources")
    endif ()
  endif ()
elseif (WIN32)
  if (MINGW)
    enable_language(ASM)
    if ( ${COMPILER_TARGET_ARCH} STREQUAL "x86_64")
      list (APPEND CONTEXT_ASM_SRCS
          ${CONTEXT_ASM_DIR}/jump_x86_64_ms_pe_clang_gas.S
          ${CONTEXT_ASM_DIR}/make_x86_64_ms_pe_clang_gas.S
           )
    elseif ( ${COMPILER_TARGET_ARCH} STREQUAL "i386")
      list (APPEND CONTEXT_ASM_SRCS
          ${CONTEXT_ASM_DIR}/jump_i386_ms_pe_clang_gas.S
          ${CONTEXT_ASM_DIR}/make_i386_ms_pe_clang_gas.S
            )
    else ()
        message (FATAL_ERROR "Unsupported WIN-platform architecture: ${COMPILER_TARGET_ARCH}. Unable to chose context sources")
    endif ()
  else ()
    enable_language(ASM_MASM)
    if ( ${COMPILER_TARGET_ARCH} STREQUAL "x86_64")
      list (APPEND CONTEXT_ASM_SRCS
           ${CONTEXT_ASM_DIR}/jump_x86_64_ms_pe_masm.asm
           ${CONTEXT_ASM_DIR}/make_x86_64_ms_pe_masm.asm
           )
    elseif ( ${COMPILER_TARGET_ARCH} STREQUAL "i386")
      list (APPEND CONTEXT_ASM_SRCS
            ${CONTEXT_ASM_DIR}/jump_i386_ms_pe_masm.asm
            ${CONTEXT_ASM_DIR}/make_i386_ms_pe_masm.asm
            )
    else ()
        message (FATAL_ERROR "Unsupported WIN-platform architecture: ${COMPILER_TARGET_ARCH}. Unable to chose context sources")
    endif ()
  endif ()
else ()
    message (FATAL_ERROR "Unsupported platform. Unable to chose context sources")
endif ()
list(APPEND SRCS ${CONTEXT_ASM_SRCS})

if (ENABLE_SSE)
  if (NOT MSVC AND NOT APPLE AND (${COMPILER_TARGET_ARCH} STREQUAL "x86_64" OR ${COMPILER_TARGET_ARCH} STREQUAL "i386"))
    add_definitions(-DREINDEXER_WITH_SSE=1)
    message ("Building with SSE support...")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -mpopcnt")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -mpopcnt")
  else ()
    message ("SSE compiler flags were disabled for the current platform")
  endif ()
endif ()

include_directories(${REINDEXER_SOURCE_PATH})
include_directories(${REINDEXER_SOURCE_PATH}/vendor)

set (MSGPACK_INCLUDE_PATH ${REINDEXER_SOURCE_PATH}/vendor/msgpack)
include_directories(${MSGPACK_INCLUDE_PATH})

set (KOISHI_PATH ${REINDEXER_SOURCE_PATH}/vendor/koishi)
if (CMAKE_GENERATOR MATCHES "Visual Studio")
  add_definitions("-DKOISHI_THREAD_LOCAL=__declspec(thread)")
else()
  add_definitions(-DKOISHI_THREAD_LOCAL=_Thread_local)
endif()
add_definitions(-DKOISHI_HAVE_MMAP)
add_definitions(-DKOISHI_MAP_ANONYMOUS=MAP_ANONYMOUS)
add_definitions(-DKOISHI_HAVE_SYSCONF)
add_definitions(-DKOISHI_SC_PAGE_SIZE=_SC_PAGE_SIZE)
add_definitions(-DKOISHI_HAVE_GETPAGESIZE)
add_definitions(-DKOISHI_HAVE_ALIGNED_ALLOC)
add_definitions(-DKOISHI_HAVE_POSIX_MEMALIGN)
add_definitions(-DBUILDING_KOISHI)
add_definitions(-DFCONTEXT_CALL=)
add_definitions(-DYAML_CPP_STATIC_DEFINE)
include_directories(${KOISHI_PATH}/include)

list(APPEND SRCS ${KOISHI_PATH}/include/koishi.h
        ${KOISHI_PATH}/fiber.h
        ${KOISHI_PATH}/stack_alloc.c
        ${KOISHI_PATH}/stack_alloc.h
)
if (${COMPILER_TARGET_ARCH} STREQUAL "e2k")
  list(APPEND SRCS ${KOISHI_PATH}/ucontext_e2k/ucontext_e2k.c)
else()
  list(APPEND SRCS ${KOISHI_PATH}/fcontext/fcontext.c ${KOISHI_PATH}/fcontext/fcontext.hpp)
endif()

# Static LevelDB v1.23 is built with -fno-rtti by default. To inherit our logger from leveldb's logger, this file must be built with -fno-rtti to
set_source_files_properties(${REINDEXER_SOURCE_PATH}/core/storage/leveldblogger.cc PROPERTIES COMPILE_FLAGS "-fno-rtti")

list(APPEND REINDEXER_LIBRARIES reindexer)
add_library(${TARGET} STATIC ${HDRS} ${SRCS} ${VENDORS})
add_definitions(-DREINDEX_CORE_BUILD=1)


# add_definitions(-DREINDEX_FT_EXTRA_DEBUG=1)

add_subdirectory(server/contrib)

## Dependencies

# tcmalloc
##########
if (NOT WITH_ASAN AND NOT WITH_TSAN)
  # tmalloc conflict with sanitizers, so disable it for sanitized builds
  if (ENABLE_TCMALLOC)
    find_package(Gperftools)
    if(GPERFTOOLS_TCMALLOC)
      include_directories(SYSTEM ${GPERFTOOLS_INCLUDE_DIR})
      add_definitions(-DREINDEX_WITH_GPERFTOOLS=1)
      list (APPEND REINDEXER_LIBRARIES ${GPERFTOOLS_LIBRARIES})
    endif()
  endif()

  if (NOT GPERFTOOLS_TCMALLOC AND ENABLE_JEMALLOC)
    find_package(Jemalloc)
    if(JEMALLOC_FOUND)
      include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
      add_definitions(-DREINDEX_WITH_JEMALLOC=1)
      list (APPEND REINDEXER_LIBRARIES ${JEMALLOC_LIBRARY})
    endif()
  endif ()
endif ()

# snappy
########
if (NOT WITH_TSAN)
  find_package(Snappy)
endif()
if (SNAPPY_FOUND)
  include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
  list(APPEND REINDEXER_LIBRARIES ${SNAPPY_LIBRARIES})
else ()
  if (WITH_TSAN)
    message (STATUS "Snappy will be downloaded from Github to avoid false-positive warnings from TSAN")
  else()
    message (STATUS "Snappy not found. Will download it")
  endif()
  ExternalProject_Add(
    snappy_lib
    GIT_REPOSITORY "https://github.com/google/snappy.git"
    GIT_TAG "1.1.8"
    CMAKE_ARGS -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_CURRENT_BINARY_DIR}
    )
  include_directories (${CMAKE_CURRENT_BINARY_DIR}/include)
  link_directories(${CMAKE_CURRENT_BINARY_DIR})
  list(APPEND REINDEXER_LIBRARIES snappy)
endif ()

# storage
#########
# rocksdb
if (ENABLE_ROCKSDB)
  if(GPERFTOOLS_TCMALLOC AND NOT WIN32 AND WITH_PYTHON)
    message (STATUS "Python connector is incompatible with both tcmalloc and RocksDB enabled. Disabling RocksDB")
  else()
    if (WITH_PYTHON)
      # librocksdb usually compiles without -fPIC, so it can't be linked to another shared library (i.e. to reindexer python bindings)
      set (RocksDB_NAMES librocksdb.so)
    elseif(GPERFTOOLS_TCMALLOC AND NOT WIN32)
      # shared version of rocksdb can conflict with tcmalloc, so force static version of rocksdb, if tcmalloc enabled
      set (RocksDB_NAMES librocksdb.a)
    endif()
    find_library(RocksDB_LIBRARY NAMES ${RocksDB_NAMES} rocksdb HINTS $ENV{ROCKSDB_ROOT}/lib)
    find_path(RocksDB_INCLUDE_DIR NAMES rocksdb/db.h HINTS $ENV{ROCKSDB_ROOT}/include /opt/local/include /usr/local/include /usr/include)
    if (RocksDB_LIBRARY AND RocksDB_INCLUDE_DIR)
      message (STATUS "Found RocksDB: ${RocksDB_LIBRARY}")
      find_library(BZ2_LIBRARY bz2 bzip2)
      if (BZ2_LIBRARY)
        message (STATUS "Found libbz2: ${BZ2_LIBRARY}")
        list(APPEND REINDEXER_LIBRARIES ${BZ2_LIBRARY})
      else()
        message (STATUS "libbz2: not found")
      endif()

      find_library(LZ4_LIBRARY lz4)
      if (LZ4_LIBRARY)
        message (STATUS "Found liblz4: ${LZ4_LIBRARY}")
        list(APPEND REINDEXER_LIBRARIES ${LZ4_LIBRARY})
      else()
        message (STATUS "liblz4: not found")
      endif()

      find_library(Z_LIBRARY z)
      if (Z_LIBRARY)
        message (STATUS "Found zlib: ${Z_LIBRARY}")
        list(APPEND REINDEXER_LIBRARIES ${Z_LIBRARY})
      else()
        message (STATUS "zlib: not found")
      endif()

      find_library(ZSTD_LIBRARY zstd)
      if (ZSTD_LIBRARY)
        message (STATUS "Found zstdlib: ${ZSTD_LIBRARY}")
        list(APPEND REINDEXER_LIBRARIES ${ZSTD_LIBRARY})
      else()
        message (STATUS "zstdlib: not found")
      endif()

      include_directories(SYSTEM ${RocksDB_INCLUDE_DIR})
      list(INSERT REINDEXER_LIBRARIES 1 ${RocksDB_LIBRARY})
      add_definitions(-DREINDEX_WITH_ROCKSDB)
    endif()
  endif()
endif()

# leveldb
if(GPERFTOOLS_TCMALLOC AND NOT WIN32)
  # shared version of leveldb can conflict with tcmalloc, so force static version of leveldb, if tcmalloc enabled
  set (LevelDB_NAMES libleveldb.a)
endif()
if (NOT WITH_TSAN)
  find_library(LevelDB_LIBRARY NAMES ${LevelDB_NAMES} leveldb HINTS $ENV{LEVELDB_ROOT}/lib)
  find_path(LevelDB_INCLUDE_DIR NAMES leveldb/db.h HINTS $ENV{LEVELDB_ROOT}/include /opt/local/include /usr/local/include /usr/include)
endif()

if (NOT LevelDB_LIBRARY OR NOT LevelDB_INCLUDE_DIR OR WITH_TSAN)
  if (WITH_TSAN)
    message (STATUS "LevelDB will be downloaded from Github to avoid false-positive warnings from TSAN")
  else ()
    # Leveldb not found. Download it
    message (STATUS "LevelDB not found. Will download it")
  endif()
  ExternalProject_Add(
    leveldb_lib
    GIT_REPOSITORY "https://github.com/restream/leveldb.git"
    GIT_TAG "master"
    CMAKE_ARGS -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
    "-DCMAKE_CXX_FLAGS=-fPIC -I${CMAKE_CURRENT_BINARY_DIR}/include"
    -DCMAKE_EXE_LINKER_FLAGS=-L${CMAKE_CURRENT_BINARY_DIR}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_CURRENT_BINARY_DIR}
  )
  if (NOT SNAPPY_FOUND)
    add_dependencies(leveldb_lib snappy_lib)
  endif()
  include_directories (${CMAKE_CURRENT_BINARY_DIR}/include)
  link_directories(${CMAKE_CURRENT_BINARY_DIR})
  list(APPEND REINDEXER_LINK_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR})
  list(INSERT REINDEXER_LIBRARIES 1 leveldb)
  add_dependencies(reindexer leveldb_lib)
else ()
  message (STATUS "Found LevelDB: ${LevelDB_LIBRARY}")
  include_directories(SYSTEM ${LevelDB_INCLUDE_DIR})
  list(INSERT REINDEXER_LIBRARIES 1 ${LevelDB_LIBRARY})
endif ()
add_definitions(-DREINDEX_WITH_LEVELDB)

# System libraries
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED ON)
list(APPEND REINDEXER_LIBRARIES ${CMAKE_THREAD_LIBS_INIT} )

if (WITH_CPPTRACE)
  ExternalProject_Add(
    cpptrace_lib
    GIT_REPOSITORY "https://github.com/jeremy-rifkin/cpptrace.git"
    GIT_TAG "v0.3.1"
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=${CMAKE_CURRENT_BINARY_DIR}	
      -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
      -DCPPTRACE_STATIC=On
      -DCPPTRACE_GET_SYMBOLS_WITH_DBGHELP=On
      -DCPPTRACE_UNWIND_WITH_DBGHELP=On
      -DCPPTRACE_DEMANGLE_WITH_WINAPI=On	
  )
  add_definitions(-DREINDEX_WITH_CPPTRACE)
  add_dependencies(reindexer cpptrace_lib)
  list(APPEND REINDEXER_LIBRARIES cpptrace ${REINDEXER_LIBRARIES})
endif()

# librt
find_library(LIBRT rt)
if(LIBRT)
  list (APPEND REINDEXER_LIBRARIES ${LIBRT})
endif()

if (NOT WIN32)
  # libdl
  find_library(LIBDL dl)
  if(LIBDL)
    list (APPEND REINDEXER_LIBRARIES ${LIBDL})
    add_definitions(-DREINDEX_WITH_LIBDL=1)
  endif()
endif()

# Unwind from libgcc or clang
include (CheckSymbolExists)
list(APPEND CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
check_symbol_exists (_Unwind_Backtrace unwind.h HAVE_BACKTRACE)
check_symbol_exists (_Unwind_GetIPInfo unwind.h HAVE_GETIPINFO)
list(REMOVE_ITEM CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
if (HAVE_BACKTRACE AND HAVE_GETIPINFO)
  set (SYSUNWIND On)
  message ("-- Found system unwind") 
  add_definitions(-DREINDEX_WITH_UNWIND=1) 
endif()
  
# libunwind
if (ENABLE_LIBUNWIND)
  find_library(LIBUNWIND unwind)
  if(LIBUNWIND)
    list (APPEND REINDEXER_LIBRARIES ${LIBUNWIND} )
    find_path(LIBUNWIND_INCLUDE_PATH libunwind.h)
    if (LIBUNWIND_INCLUDE_PATH)
      add_definitions(-DREINDEX_WITH_LIBUNWIND=1)
      message ("-- Found Libunwind: ${LIBUNWIND} ${LIBUNWIND_INCLUDE_PATH}") 
    endif()
  endif()
endif ()

if(APPLE OR (NOT LIBUNWIND AND NOT SYSUNWIND))
  # Try execinfo
  find_path(EXECINFO_INCLUDE_PATH execinfo.h)
  if (EXECINFO_INCLUDE_PATH)
    message ("-- Found execinfo.h: ${EXECINFO_INCLUDE_PATH}") 
    add_definitions(-DREINDEX_WITH_EXECINFO=1)
    find_library(LIBEXECINFO execinfo)
    if(LIBEXECINFO)
      list (APPEND REINDEXER_LIBRARIES ${LIBEXECINFO})
    endif()
  endif()
endif ()

find_library(MUSL ld-musl-x86_64.so.1)
if (MUSL)
  message ("-- Found musl, will override abort and assert_fail to fix stacktraces") 
  add_definitions(-DREINDEX_OVERRIDE_ABORT=1)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

if (APPLE)
  add_definitions (-DREINDEX_WITH_APPLE_SYMBOLICATION=1)
endif()

if (WIN32)
  list(APPEND REINDEXER_LIBRARIES shlwapi dbghelp ws2_32)
endif ()

set (REINDEXER_LIBRARIES_GLOBAL ${REINDEXER_LIBRARIES} PARENT_SCOPE)
set (REINDEXER_LINK_DIRECTORIES_GLOBAL ${REINDEXER_LINK_DIRECTORIES} PARENT_SCOPE)

# Get version string
if(EXISTS ${PROJECT_SOURCE_DIR}/.git OR EXISTS ${PROJECT_SOURCE_DIR}/../.git )
  execute_process(WORKING_DIRECTORY ${REINDEXER_SOURCE_PATH} COMMAND git describe --tags OUTPUT_VARIABLE REINDEXER_VERSION_FULL OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

if (NOT REINDEXER_VERSION_FULL)
set (REINDEXER_VERSION_FULL ${REINDEXER_VERSION_DEFAULT})
endif ()

set (REINDEXER_VERSION_H "#pragma once\n#define REINDEX_VERSION \"${REINDEXER_VERSION_FULL}\"\n")

if(EXISTS ${PROJECT_BINARY_DIR}/reindexer_version.h)
   file(READ ${PROJECT_BINARY_DIR}/reindexer_version.h REINDEXER_VERSION_CUR_H)
endif ()

if (NOT REINDEXER_VERSION_CUR_H STREQUAL REINDEXER_VERSION_H)
  file(WRITE ${PROJECT_BINARY_DIR}/reindexer_version.h ${REINDEXER_VERSION_H})
endif ()

include_directories(${PROJECT_BINARY_DIR})

string ( REGEX REPLACE "(.*)([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)" "\\2.\\3.\\4" REINDEXER_VERSION ${REINDEXER_VERSION_FULL})
string ( REGEX REPLACE ".*-([0-9]+)-(.*)" "\\1.\\2" REINDEXER_RELEASE ${REINDEXER_VERSION_FULL})
set (REINDEXER_VERSION_REDUCED ${REINDEXER_VERSION})
if (CMAKE_MATCH_1)
  set (REINDEXER_VERSION ${REINDEXER_VERSION}.${REINDEXER_RELEASE})
endif()

# Packing and installation
include (RxPrepareCpackDeps)
include (RxPrepareInstallFiles)

# Build subdirectories
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

find_package(benchmark)
if (benchmark_FOUND)
  if (${benchmark_VERSION_MAJOR} EQUAL "1" AND ${benchmark_VERSION_MINOR} GREATER_EQUAL "5" AND ${benchmark_VERSION_MINOR} LESS_EQUAL "7")
    find_package(GBenchmark)
    if (GBENCHMARK_FOUND)
      include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})
      add_subdirectory(gtests/bench)
    endif()
  else()
    message (STATUS "Unsupported Google benchmark version: ${benchmark_VERSION}. Only versions 1.5.x-1.7.x are supported")
  endif()
endif()

add_custom_target(collect_coverage
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
  COMMAND lcov --directory . --capture -o coverage.info
  COMMAND lcov --remove coverage.info '/opt/rh/devtoolset*' '/usr/*' '/Library/*' '${PROJECT_SOURCE_DIR}/vendor/*' '${PROJECT_BINARY_DIR}/gtests/tests/*' -o coverage_filtered.info
  COMMAND genhtml coverage_filtered.info -o coverage_output
  COMMENT "Collecting Reindexer coverage"
)
