cmake_minimum_required(VERSION 3.18)

# Scenario helper for the process-wide behaviour of the embedding module loader. CTest runs it once per scenario with its own
# RX_CUSTOM_EMBEDDING_LIB_NAME, which is also how the stub is attached; the reason for one process per scenario is in
# embedding_scenarios.cc. The helper depends on the stubs, so a plain build produces everything the registered tests need.

project(embedding_scenarios)

set(TARGET embedding_scenarios)

add_executable(${TARGET} ${CMAKE_CURRENT_SOURCE_DIR}/embedding_scenarios.cc)
add_dependencies(${TARGET} embedding_stubs)
target_include_directories(${TARGET} PRIVATE ${REINDEXER_SOURCE_PATH} ${REINDEXER_SOURCE_PATH}/vendor)
target_link_libraries(${TARGET} reindexer ${REINDEXER_LIBRARIES})
# Readable stack traces, same as the gtest targets.
set_property(TARGET ${TARGET} PROPERTY ENABLE_EXPORTS 1)

set(EMBEDDING_MODELS_DIR "${REINDEXER_SOURCE_PATH}/gtests/tests_data/embedders/models")

# A scenario that expects no library at all still has to name one, otherwise it would pick up whatever the platform search finds.
set(EMBEDDING_MISSING_LIB "${CMAKE_CURRENT_BINARY_DIR}/no-such-embedding-library.so")

# EMPTY_LIBRARY is a separate keyword rather than LIBRARY "": cmake_parse_arguments does not set a one value keyword whose value is the
# empty string, so the empty override would silently become no override at all.
function(rx_add_embedding_scenario NAME)
  cmake_parse_arguments(SCENARIO "EMPTY_LIBRARY" "LIBRARY" "" ${ARGN})
  add_test(NAME embedding_module.${NAME} COMMAND ${TARGET} ${NAME} ${EMBEDDING_MODELS_DIR}/good)
  set(SCENARIO_ENV "")
  if (SCENARIO_EMPTY_LIBRARY)
    list(APPEND SCENARIO_ENV "RX_CUSTOM_EMBEDDING_LIB_NAME=")
  elseif (SCENARIO_LIBRARY)
    list(APPEND SCENARIO_ENV "RX_CUSTOM_EMBEDDING_LIB_NAME=${SCENARIO_LIBRARY}")
  endif()
  if (WITH_TSAN)
    list(APPEND SCENARIO_ENV "TSAN_OPTIONS=suppressions=${REINDEXER_SOURCE_PATH}/gtests/tsan.suppressions")
  endif()
  if (WITH_UBSAN)
    list(APPEND SCENARIO_ENV "UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1")
  endif()
  if (SCENARIO_ENV)
    set_tests_properties(embedding_module.${NAME} PROPERTIES ENVIRONMENT "${SCENARIO_ENV}")
  endif()
endfunction()

rx_add_embedding_scenario(absent LIBRARY "${EMBEDDING_MISSING_LIB}")
# A configuration mistake and a missing library at once: the mistake has to win.
rx_add_embedding_scenario(invalid_config_without_library LIBRARY "${EMBEDDING_MISSING_LIB}")
# The override is present but empty, which is a configuration mistake rather than a request for the platform name. Only on POSIX: on
# Windows an environment variable set to an empty string does not exist, so this branch of the loader is unreachable there.
if (NOT WIN32)
  rx_add_embedding_scenario(empty_override EMPTY_LIBRARY)
endif()
# No override, so the platform name is used; nothing installs a library under it during the tests.
rx_add_embedding_scenario(platform_default)
rx_add_embedding_scenario(unsupported_abi LIBRARY "$<TARGET_FILE:embedding_stub_unsupported_abi>")
rx_add_embedding_scenario(short_table LIBRARY "$<TARGET_FILE:embedding_stub_short_table>")
rx_add_embedding_scenario(null_member LIBRARY "$<TARGET_FILE:embedding_stub_null_member>")
rx_add_embedding_scenario(valid LIBRARY "$<TARGET_FILE:embedding_stub>")
# A table larger than the required prefix, as a module built from a newer header reports it: the known prefix must still be accepted.
rx_add_embedding_scenario(extended_abi LIBRARY "$<TARGET_FILE:embedding_stub_extended_abi>")
# The first loaded model fixes the process-wide thread count; a value out of range is refused by the call that passes it.
rx_add_embedding_scenario(first_load_wins LIBRARY "$<TARGET_FILE:embedding_stub>")
rx_add_embedding_scenario(invalid_thread_count LIBRARY "$<TARGET_FILE:embedding_stub>")
# model_load before init. Needs its own process: in the common test binary something has already started the module.
rx_add_embedding_scenario(abi_load_before_init LIBRARY "$<TARGET_FILE:embedding_stub>")
# init with no options at all. Needs its own process: the one successful init of a process is spent on it.
rx_add_embedding_scenario(abi_init_default_options LIBRARY "$<TARGET_FILE:embedding_stub>")
rx_add_embedding_scenario(broken_library LIBRARY "${REINDEXER_SOURCE_PATH}/core/embedding/rx_embed.h")
rx_add_embedding_scenario(no_version_string LIBRARY "$<TARGET_FILE:embedding_stub_no_version>")
rx_add_embedding_scenario(init_fails LIBRARY "$<TARGET_FILE:embedding_stub_init_fails>")
# The module is started once per process, and the loader must not ask twice.
rx_add_embedding_scenario(init_happens_once LIBRARY "$<TARGET_FILE:embedding_stub>")
# Several threads race for the first load; nothing serializes them above the loader.
rx_add_embedding_scenario(concurrent_first_load LIBRARY "$<TARGET_FILE:embedding_stub>")
# The layer the facade adds: module statuses become Reindexer error codes, and the per-model configuration is refused before the ABI.
rx_add_embedding_scenario(status_mapping LIBRARY "$<TARGET_FILE:embedding_stub>")
rx_add_embedding_scenario(model_config_rejected LIBRARY "$<TARGET_FILE:embedding_stub>")
