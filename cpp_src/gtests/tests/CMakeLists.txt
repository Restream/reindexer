cmake_minimum_required(VERSION 3.10)

project(tests)

SET(TARGET tests)
SET(FUZZING_TARGET fuzzing)
list(APPEND REINDEXER_LIBRARIES reindexer_server_library reindexer ${REINDEXER_LIBRARIES})

option(XML_REPORT "Enable XML xUnit report" OFF)

include_directories(fixtures mocks)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${REINDEXER_SOURCE_PATH})

set(GENERATED_PROTO_DIR "${PROJECT_BINARY_DIR}")
if (ENABLE_GRPC)
    find_package(Protobuf)
    if (PROTOBUF_FOUND)
        INCLUDE_DIRECTORIES(SYSTEM ${PROTOBUF_INCLUDE_DIR})
        list(APPEND REINDEXER_LIBRARIES ${PROTOBUF_LIBRARIES})
        file(GLOB PROTOBUF_SOURCE_FILES ${REINDEXER_SOURCE_PATH}/gtests/tests/proto/*.proto)

        SET(PROTOBUF_PROTOC_EXECUTABLE protoc)

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
        message("Protobuf not found")
    endif()
endif()


file(GLOB_RECURSE SRCS *.cc *.h ${GENERATED_PROTO_DIR}/*.cc)
file(GLOB_RECURSE FUZZING_SRCS */fuzzing/* *fuzzing/*)
list(REMOVE_ITEM SRCS ${FUZZING_SRCS})

add_executable(${TARGET} ${SRCS})
add_executable(${FUZZING_TARGET} ${FUZZING_SRCS})

# Enable export to provide readable stacktraces
set_property(TARGET ${TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${FUZZING_TARGET} PROPERTY ENABLE_EXPORTS 1)

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARIES})
target_link_libraries(${FUZZING_TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARIES})
if (ENABLE_GRPC)
    add_definitions(-DWITH_GRPC)
    target_link_libraries(${TARGET} reindexer_grpc_library)
endif()

set(GTEST_TIMEOUT 600)
if (WITH_ASAN OR WITH_TSAN OR WITH_STDLIB_DEBUG)
    set(GTEST_TIMEOUT 2000)
endif()

find_program(GTEST_PARALLEL "gtest-parallel")
if (GTEST_PARALLEL)
    if (XML_REPORT)
        add_test(NAME gtests COMMAND gtest-parallel --gtest_color=no --serialize_test_cases --print_test_times --timeout ${GTEST_TIMEOUT} --gtest_output=xml:${REINDEXER_SOURCE_PATH}/testReport.xml "./${TARGET}")
    else()
        add_test(NAME gtests COMMAND gtest-parallel --serialize_test_cases --print_test_times --timeout ${GTEST_TIMEOUT} "./${TARGET}")
    endif()
else()
    add_test(NAME gtests COMMAND ${TARGET} --gtest_color=yes)
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

