cmake_minimum_required(VERSION 2.8)

project(tests)

SET(TARGET tests)
list(APPEND REINDEXER_LIBRARIES reindexer_server_library reindexer ${REINDEXER_LIBRARIES})

include_directories(fixtures mocks)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${REINDEXER_SOURCE_PATH})

find_package(Protobuf)
if(PROTOBUF_FOUND)
    INCLUDE_DIRECTORIES(SYSTEM ${PROTOBUF_INCLUDE_DIR})
    list(APPEND REINDEXER_LIBRARIES ${PROTOBUF_LIBRARIES})

    file(GLOB PROTOBUF_SOURCE_FILES ${REINDEXER_SOURCE_PATH}/gtests/tests/proto/*.proto)
    set(GENERATED_PROTO_DIR "${REINDEXER_SOURCE_PATH}/../build/generated/proto")
    file(MAKE_DIRECTORY ${GENERATED_PROTO_DIR})

    SET(PROTOBUF_PROTOC_EXECUTABLE protoc)

    foreach(CUR_PROTO_FILE ${PROTOBUF_SOURCE_FILES})
        get_filename_component(PROTOBUF_INPUT_DIRECTORY "${CUR_PROTO_FILE}" DIRECTORY)
        get_filename_component(PROTOBUF_INPUT_DIRECTORY "${PROTOBUF_INPUT_DIRECTORY}" ABSOLUTE)
        get_filename_component(PROTO_FILENAME "${CUR_PROTO_FILE}" NAME)
        get_filename_component(CUR_PROTO_FILE "${CUR_PROTO_FILE}" ABSOLUTE)
        string(REGEX REPLACE ".proto" ".pb" CUR_FILE_OUT "${PROTO_FILENAME}")
        execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --proto_path=${PROTOBUF_INPUT_DIRECTORY} --cpp_out=${GENERATED_PROTO_DIR} ${CUR_PROTO_FILE})
        set_source_files_properties(${GENERATED_PROTO_DIR}/${CUR_FILE_OUT}.h ${GENERATED_PROTO_DIR}/${CUR_FILE_OUT}.cc PROPERTIES COMPILE_FLAGS "-Wno-all -Wno-extra -Wno-error -Wno-old-style-cast")
        target_sources(reindexer PRIVATE ${GENERATED_PROTO_DIR}/${CUR_FILE_OUT}.cc)
    endforeach(CUR_PROTO_FILE)
    target_compile_definitions(reindexer PUBLIC WITH_PROTOBUF=1)
else()
    message ("Protobuf not found")
endif()

file (GLOB_RECURSE SRCS *.cc *.h)

add_executable(${TARGET} ${SRCS})

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} ${GTEST_LIBRARY})

find_program (GTEST_PARALLEL "gtest-parallel")
if (GTEST_PARALLEL)
  add_test (NAME gtests COMMAND gtest-parallel --serialize_test_cases --print_test_times "./${TARGET}" )
else()
  add_test (NAME gtests COMMAND ${TARGET} --gtest_color=yes )
endif()

if (WITH_TSAN)
  set_tests_properties( gtests PROPERTIES ENVIRONMENT TSAN_OPTIONS=suppressions=${REINDEXER_SOURCE_PATH}/gtests/tsan.suppressions )
endif ()
