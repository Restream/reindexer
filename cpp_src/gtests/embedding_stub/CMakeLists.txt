cmake_minimum_required(VERSION 3.18)

# Test stub of the embedding module: several shared libraries from the same sources, one working and the rest each breaking the contract in
# one way. What each variant does is in rx_embed_stub.cc. They are EXCLUDE_FROM_ALL and have no install() rule, so they are built only when
# something that is being built needs them, and never packaged. The directory is registered unconditionally, unlike gtests/tests, so that
# `embedding_stubs` can be built by name without gtest installed.

project(embedding_stub)

# The stub parses metadata.json with the vendored gason, like the real module. gason is not standalone here: it calls
# reindexer::fail_assertrx and reindexer::ToLower, whose closure reaches most of the core. Three settings keep that closure out and belong
# together: NDEBUG makes the assertrx uses in gason.h no-ops, function and data sections with garbage collection drop the only user of
# ToLower, and linking with no undefined symbols allowed turns future growth of the closure into a link error.
set(EMBEDDING_STUB_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/rx_embed_stub.cc
  ${REINDEXER_SOURCE_PATH}/vendor/gason/gason.cc)
file(GLOB EMBEDDING_STUB_DOUBLE_CONVERSION ${REINDEXER_SOURCE_PATH}/vendor/double-conversion/*.cc)
list(APPEND EMBEDDING_STUB_SRCS ${EMBEDDING_STUB_DOUBLE_CONVERSION})

# The tool that lists the exports of a built library: dumpbin under MSVC, nm everywhere else.
if (MSVC)
  find_program(RX_EMBEDDING_NM NAMES dumpbin)
  set(RX_EMBEDDING_EXPORT_TOOL dumpbin)
else()
  find_program(RX_EMBEDDING_NM NAMES nm llvm-nm)
  set(RX_EMBEDDING_EXPORT_TOOL nm)
endif()
if (NOT RX_EMBEDDING_NM)
  message(WARNING "${RX_EMBEDDING_EXPORT_TOOL} was not found: the embedding stubs will not be checked for extra exported symbols")
endif()

set(EMBEDDING_STUB_VERSION_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/rx_embed_stub.version)
set(EMBEDDING_STUB_SYMBOLS_LIST ${CMAKE_CURRENT_SOURCE_DIR}/rx_embed_stub.symbols)
set(EMBEDDING_STUB_DEF_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rx_embed_stub.def)

set(EMBEDDING_STUB_TARGETS "")

function(rx_add_embedding_stub STUB_TARGET)
  add_library(${STUB_TARGET} SHARED EXCLUDE_FROM_ALL ${EMBEDDING_STUB_SRCS})
  set(EMBEDDING_STUB_TARGETS ${EMBEDDING_STUB_TARGETS} ${STUB_TARGET} PARENT_SCOPE)

  target_include_directories(${STUB_TARGET} PRIVATE ${REINDEXER_SOURCE_PATH} ${REINDEXER_SOURCE_PATH}/vendor)
  # Under MSVC the export table comes from the module definition file, so dllexport must not be added on top of it.
  set(STUB_DEFINITIONS NDEBUG FMT_HEADER_ONLY=1 ${ARGN})
  if (NOT MSVC)
    list(APPEND STUB_DEFINITIONS RX_EMBED_BUILDING_LIBRARY=1)
  endif()
  target_compile_definitions(${STUB_TARGET} PRIVATE ${STUB_DEFINITIONS})

  set_target_properties(${STUB_TARGET} PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_VISIBILITY_PRESET hidden
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
    WINDOWS_EXPORT_ALL_SYMBOLS OFF)

  # The single exported symbol rests on one build setting, so it is checked rather than trusted.
  if (RX_EMBEDDING_NM)
    add_custom_command(TARGET ${STUB_TARGET} POST_BUILD
      COMMAND ${CMAKE_COMMAND} -DLIBRARY=$<TARGET_FILE:${STUB_TARGET}> -DEXPECTED_SYMBOL=rx_embed_get_api_base
      -DNM=${RX_EMBEDDING_NM} -DTOOL=${RX_EMBEDDING_EXPORT_TOOL} -P ${CMAKE_CURRENT_SOURCE_DIR}/RxVerifyExports.cmake
      COMMENT "Checking that ${STUB_TARGET} exports only rx_embed_get_api_base"
      VERBATIM)
  endif()

  if (MSVC)
    # The module definition file names the entry point undecorated; dllexport would decorate a __cdecl function on x86.
    target_sources(${STUB_TARGET} PRIVATE ${EMBEDDING_STUB_DEF_FILE})
    set_target_properties(${STUB_TARGET} PROPERTIES LINK_DEPENDS ${EMBEDDING_STUB_DEF_FILE})
    target_link_options(${STUB_TARGET} PRIVATE /OPT:REF)
  else()
    target_compile_options(${STUB_TARGET} PRIVATE -ffunction-sections -fdata-sections)
    if (APPLE)
      # TODO: the real module targets Linux first, so this branch is only exercised by the stub itself.
      target_link_options(${STUB_TARGET} PRIVATE -Wl,-dead_strip -Wl,-exported_symbols_list,${EMBEDDING_STUB_SYMBOLS_LIST})
      set_target_properties(${STUB_TARGET} PROPERTIES LINK_DEPENDS ${EMBEDDING_STUB_SYMBOLS_LIST})
    else()
      target_link_options(${STUB_TARGET} PRIVATE
        -Wl,--gc-sections
        -Wl,--no-undefined
        -Wl,--version-script=${EMBEDDING_STUB_VERSION_SCRIPT})
      set_target_properties(${STUB_TARGET} PROPERTIES LINK_DEPENDS ${EMBEDDING_STUB_VERSION_SCRIPT})
    endif()
  endif()
endfunction()

rx_add_embedding_stub(embedding_stub)
rx_add_embedding_stub(embedding_stub_unsupported_abi RX_EMBED_STUB_UNSUPPORTED_ABI=1)
rx_add_embedding_stub(embedding_stub_extended_abi RX_EMBED_STUB_EXTENDED_ABI=1)
rx_add_embedding_stub(embedding_stub_short_table RX_EMBED_STUB_BROKEN_ABI=1)
rx_add_embedding_stub(embedding_stub_null_member RX_EMBED_STUB_BROKEN_ABI=2)
rx_add_embedding_stub(embedding_stub_no_version RX_EMBED_STUB_NO_VERSION_STRING=1)
rx_add_embedding_stub(embedding_stub_init_fails RX_EMBED_STUB_INIT_FAILS=1)

# Keeps the contract compiling as plain C. Not EXCLUDE_FROM_ALL, so an ordinary build checks it.
add_library(embedding_abi_c_compat OBJECT ${CMAKE_CURRENT_SOURCE_DIR}/rx_embed_c_compat.c)
target_include_directories(embedding_abi_c_compat PRIVATE ${REINDEXER_SOURCE_PATH})
set_target_properties(embedding_abi_c_compat PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  POSITION_INDEPENDENT_CODE ON)
if (NOT MSVC)
  target_compile_options(embedding_abi_c_compat PRIVATE -Wall -Wextra -Werror -pedantic)
endif()

# All the libraries under one name, for anyone who wants them without the tests that use them.
add_custom_target(embedding_stubs)
add_dependencies(embedding_stubs ${EMBEDDING_STUB_TARGETS})

