cmake_minimum_required(VERSION 3.18)

project(tests)

SET(TARGET tests)
SET(FUZZING_TARGET fuzzing)
list(APPEND REINDEXER_LIBRARIES reindexer_server_library reindexer ${REINDEXER_LIBRARIES})

option(XML_REPORT "Enable XML xUnit report" OFF)
option(GH_CI_OSX "Option for Github CI on OSX" OFF)
option(GTEST_SKIP_ASAN_MEMORY_HEAVY "Skips tests with high memory consumation in ASAN-build (mostly sharding-related tests). Option for Github tests with ASAN - free runners run out of memory" OFF)
set(GTEST_WORKERS "6" CACHE STRING "Number of workers for gtest_parallel")

include_directories(fixtures mocks)
include_directories(${REINDEXER_SOURCE_PATH})

set(GENERATED_PROTO_DIR "${PROJECT_BINARY_DIR}")
if(ENABLE_GRPC)
    set(protobuf_MODULE_COMPATIBLE TRUE)
    find_package(Protobuf CONFIG)
    if(NOT PROTOBUF_FOUND)
        find_package(Protobuf)
    endif()
    if(PROTOBUF_FOUND)
        include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})
        list(APPEND REINDEXER_LIBRARIES protobuf::libprotobuf)
        file(GLOB PROTOBUF_SOURCE_FILES ${REINDEXER_SOURCE_PATH}/gtests/tests/proto/*.proto)

        set(PROTOBUF_PROTOC_EXECUTABLE protoc)

        foreach (CUR_PROTO_FILE ${PROTOBUF_SOURCE_FILES})
            get_filename_component(PROTOBUF_INPUT_DIRECTORY "${CUR_PROTO_FILE}" DIRECTORY)
            get_filename_component(PROTOBUF_INPUT_DIRECTORY "${PROTOBUF_INPUT_DIRECTORY}" ABSOLUTE)
            get_filename_component(PROTO_FILENAME "${CUR_PROTO_FILE}" NAME)
            get_filename_component(CUR_PROTO_FILE "${CUR_PROTO_FILE}" ABSOLUTE)
            string(REGEX REPLACE ".proto" ".pb" CUR_FILE_OUT "${PROTO_FILENAME}")
            execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --proto_path=${PROTOBUF_INPUT_DIRECTORY} --cpp_out=${GENERATED_PROTO_DIR} ${CUR_PROTO_FILE})
            set_source_files_properties(${GENERATED_PROTO_DIR}/${CUR_FILE_OUT}.h ${GENERATED_PROTO_DIR}/${CUR_FILE_OUT}.cc PROPERTIES COMPILE_FLAGS "-Wno-all -Wno-extra -Wno-error -Wno-old-style-cast")
        endforeach (CUR_PROTO_FILE)
        add_definitions(-DWITH_PROTOBUF=1)
        include_directories(${REINDEXER_BINARY_PATH}/server/grpc ${GENERATED_PROTO_DIR})
    else()
        message("tests: Protobuf not found")
    endif()
endif()


file(GLOB_RECURSE SRCS *.cc *.h ${GENERATED_PROTO_DIR}/*.cc)
file(GLOB_RECURSE FUZZING_SRCS */fuzzing/* *fuzzing/*)
list(REMOVE_ITEM SRCS ${FUZZING_SRCS})

add_executable(${TARGET} ${SRCS})
add_executable(${FUZZING_TARGET} ${FUZZING_SRCS})

if(ENABLE_PCH)
  set(PREC_HDRS
    <vector>
    <map>
    <unordered_map>
    <set>
    <unordered_set>
    <deque>
    <memory>
    <string>
    <string_view>
    <optional>
    <chrono>
    <functional>
    <thread>
    <chrono>
    <atomic>
    <fstream>

    ${REINDEXER_SOURCE_PATH}/estl/h_vector.h
    ${REINDEXER_SOURCE_PATH}/estl/fast_hash_set.h
    ${REINDEXER_SOURCE_PATH}/estl/fast_hash_map.h
    ${REINDEXER_SOURCE_PATH}/estl/intrusive_ptr.h
    ${REINDEXER_SOURCE_PATH}/estl/mutex.h
    ${REINDEXER_SOURCE_PATH}/estl/shared_mutex.h
    ${REINDEXER_SOURCE_PATH}/estl/cow.h
    ${REINDEXER_SOURCE_PATH}/tools/errors.h
    ${REINDEXER_SOURCE_PATH}/tools/logger.h
    ${REINDEXER_SOURCE_PATH}/tools/lsn.h
    ${REINDEXER_SOURCE_PATH}/tools/serializer.h
    ${REINDEXER_SOURCE_PATH}/tools/stringstools.h
    ${REINDEXER_SOURCE_PATH}/tools/fsops.h

    ${REINDEXER_SOURCE_PATH}/core/reindexer.h
    ${REINDEXER_SOURCE_PATH}/core/query/query.h
    ${REINDEXER_SOURCE_PATH}/core/queryresults/queryresults.h
    ${REINDEXER_SOURCE_PATH}/core/item.h
    ${REINDEXER_SOURCE_PATH}/core/system_ns_names.h
    ${REINDEXER_SOURCE_PATH}/client/reindexer.h
    ${REINDEXER_SOURCE_PATH}/client/queryresults.h
    ${REINDEXER_SOURCE_PATH}/client/item.h
    ${REINDEXER_SOURCE_PATH}/cluster/config.h
    ${REINDEXER_SOURCE_PATH}/vendor/gason/gason.h
    ${REINDEXER_SOURCE_PATH}/vendor/yaml-cpp/yaml.h
    ${REINDEXER_SOURCE_PATH}/core/cjson/jsonbuilder.h

    gtest_cout.h
    fixtures/reindexer_api.h
    fixtures/ft_api.h
    fixtures/cluster_operation_api.h
    fixtures/servercontrol.h

    <gtest/gtest.h>
    <gtest/gtest-param-test.h>
  )

  target_precompile_headers(${TARGET} PRIVATE ${PREC_HDRS})
endif()

# Enable export to provide readable stacktraces
set_property(TARGET ${TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${FUZZING_TARGET} PROPERTY ENABLE_EXPORTS 1)

if (APPLE)
  get_target_property(GTEST_INCLUDE_DIRS GTest::gtest INTERFACE_INCLUDE_DIRECTORIES)
  target_compile_options(${TARGET} BEFORE PRIVATE "-isystem" ${GTEST_INCLUDE_DIRS})
  target_compile_options(${FUZZING_TARGET} BEFORE PRIVATE "-isystem" ${GTEST_INCLUDE_DIRS})
endif()

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARIES})
target_link_libraries(${FUZZING_TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARIES})
if (ENABLE_GRPC)
    add_definitions(-DWITH_GRPC)
    target_link_libraries(${TARGET} reindexer_grpc_library)
endif()

if(GH_CI_OSX)
    add_definitions(-DRX_LONG_REPLICATION_TIMEOUT)
endif()

set(GTEST_TIMEOUT 800)
if (WITH_ASAN OR WITH_TSAN OR WITH_STDLIB_DEBUG)
    set(GTEST_TIMEOUT 2000)
endif()

if(GTEST_SKIP_ASAN_MEMORY_HEAVY)
    set(GTEST_EXTRA_OPTIONS "--gtest_filter=-Sharding*:ReplicationLoadApi.DynamicRoleSwitch:CascadeReplicationApi.ForceSyncStress")
endif()
find_program(GTEST_PARALLEL "gtest-parallel")
if (GTEST_PARALLEL)
    if(XML_REPORT)
        add_test(NAME gtests COMMAND gtest-parallel --gtest_color=no --serialize_test_cases --print_test_times --timeout ${GTEST_TIMEOUT} --gtest_output=xml:${REINDEXER_SOURCE_PATH}/testReport.xml ${GTEST_EXTRA_OPTIONS} "./${TARGET}")
    else()
        add_test(NAME gtests COMMAND gtest-parallel --serialize_test_cases --print_test_times --timeout ${GTEST_TIMEOUT} ${GTEST_EXTRA_OPTIONS} "./${TARGET}")
    endif()
else()
    add_test(NAME gtests COMMAND ${TARGET} --gtest_color=yes ${GTEST_EXTRA_OPTIONS})
endif()

if (WITH_TSAN)
    set_tests_properties(gtests PROPERTIES ENVIRONMENT TSAN_OPTIONS=suppressions=${REINDEXER_SOURCE_PATH}/gtests/tsan.suppressions)
endif()

add_definitions(-DREINDEXER_TESTS_DATA_PATH="${REINDEXER_SOURCE_PATH}/gtests/tests_data")
if (ENABLE_SERVER_AS_PROCESS_IN_TEST)
    if (CMAKE_SYSTEM_NAME MATCHES "Linux")
        add_definitions(-DREINDEXER_WITH_SC_AS_PROCESS)
        add_definitions(-DREINDEXER_SERVER_PATH="$<TARGET_FILE:reindexer_server>")
    else()
        message(WARNING "\nENABLE_SERVER_AS_PROCESS_IN_TEST is set but the OS is not linux. Option disabled!\n")
    endif()
endif()

if(ENABLE_SERVER_AS_PROCESS_IN_TEST)
    if(CMAKE_SYSTEM_NAME MATCHES "Linux")	
        add_definitions(-DREINDEXER_WITH_SC_AS_PROCESS)    	
	    add_definitions(-DREINDEXER_SERVER_PATH="$<TARGET_FILE:reindexer_server>")
    else()
        message(WARNING "\nENABLE_SERVER_AS_PROCESS_IN_TEST is set but the OS is not linux. Option disabled!\n")
    endif()
endif()

