cmake_minimum_required(VERSION 3.0)

project(reindexer_grpc_library)
set (TARGET reindexer_grpc_library)


set(GENERATED_PROTO_DIR "${PROJECT_BINARY_DIR}")
set(GENERATED_TESTS_DIR "${REINDEXER_SOURCE_PATH}/../qa_tests/tests/grpc")

if (ENABLE_GRPC)
    find_package(Threads REQUIRED)

    set(protobuf_MODULE_COMPATIBLE TRUE)
    find_package(Protobuf REQUIRED)
    set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
    SET(PROTOBUF_PROTOC_EXECUTABLE protoc)
    INCLUDE_DIRECTORIES(SYSTEM ${PROTOBUF_INCLUDE_DIR})
    list(APPEND GRPC_LIBRARIES ${PROTOBUF_LIBRARIES} ${_PROTOBUF_LIBPROTOBUF})
    message(STATUS "Using protobuf ${Protobuf_VERSION}")

    find_package(gRPC ${GRPC_PACKAGE_PROVIDER} REQUIRED)
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
    set(_GRPC_GRPCPP_UNSECURE gRPC::grpc++)
    set(_REFLECTION gRPC::grpc++_reflection)
    list(APPEND GRPC_LIBRARIES ${_REFLECTION} ${_GRPC_GRPCPP_UNSECURE})
    message(STATUS "Using gRPC ${gRPC_VERSION}")

    file(GLOB PROTOBUF_SOURCE_FILES ${REINDEXER_SOURCE_PATH}/server/proto/*.proto)
    file(MAKE_DIRECTORY ${GENERATED_PROTO_DIR})
    file(MAKE_DIRECTORY ${GENERATED_TESTS_DIR})

    foreach(CUR_PROTO_FILE ${PROTOBUF_SOURCE_FILES})
        get_filename_component(PROTOBUF_INPUT_DIRECTORY "${CUR_PROTO_FILE}" DIRECTORY)
        get_filename_component(PROTOBUF_INPUT_DIRECTORY "${PROTOBUF_INPUT_DIRECTORY}" ABSOLUTE)
        get_filename_component(PROTO_FILENAME "${CUR_PROTO_FILE}" NAME)
        get_filename_component(CUR_PROTO_FILE "${CUR_PROTO_FILE}" ABSOLUTE)
        string(REGEX REPLACE ".proto" ".pb" CUR_PB_FILE_OUT "${PROTO_FILENAME}")
        string(REGEX REPLACE ".proto" ".grpc.pb" CUR_GRPC_FILE_OUT "${PROTO_FILENAME}")
        string(REGEX REPLACE ".proto" "_pb2" CUR_PB_PYTHON_FILE_OUT "${PROTO_FILENAME}")
        string(REGEX REPLACE ".proto" "_pb2_grpc" CUR_GRPC_PYTHON_FILE_OUT "${PROTO_FILENAME}")
        execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --proto_path=${PROTOBUF_INPUT_DIRECTORY} --grpc_out=${GENERATED_PROTO_DIR} --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN} --cpp_out=${GENERATED_PROTO_DIR}  ${CUR_PROTO_FILE})
        execute_process(COMMAND python3 -m grpc_tools.protoc -I${PROTOBUF_INPUT_DIRECTORY} --python_out=${GENERATED_TESTS_DIR} --grpc_python_out=${GENERATED_TESTS_DIR}  ${CUR_PROTO_FILE})
        set_source_files_properties(${GENERATED_PROTO_DIR}/${CUR_GRPC_FILE_OUT}.h ${GENERATED_PROTO_DIR}/${CUR_GRPC_FILE_OUT}.cc PROPERTIES COMPILE_FLAGS "-Wno-all -Wno-extra -Wno-error -Wno-old-style-cast")
        set_source_files_properties(${GENERATED_PROTO_DIR}/${CUR_PB_FILE_OUT}.h ${GENERATED_PROTO_DIR}/${CUR_PB_FILE_OUT}.cc PROPERTIES COMPILE_FLAGS "-Wno-all -Wno-extra -Wno-error -Wno-old-style-cast")
        if(EXISTS "${GENERATED_PROTO_DIR}/${CUR_PB_FILE_OUT}.cc")
            message("gRpc success: ${GENERATED_PROTO_DIR}/${CUR_PB_FILE_OUT}.cc")
        endif()
        if(EXISTS "${GENERATED_PROTO_DIR}/${CUR_GRPC_FILE_OUT}.cc")
            message("gRpc success: ${GENERATED_PROTO_DIR}/${CUR_GRPC_FILE_OUT}.cc")
        endif()
        if(EXISTS "${GENERATED_TESTS_DIR}/${CUR_PB_PYTHON_FILE_OUT}.py")
            message("gRpc success: ${GENERATED_TESTS_DIR}/${CUR_PB_PYTHON_FILE_OUT}.py")
        endif()
        if(EXISTS "${GENERATED_TESTS_DIR}/${CUR_GRPC_PYTHON_FILE_OUT}.py")
            message("gRpc success: ${GENERATED_TESTS_DIR}/${CUR_GRPC_PYTHON_FILE_OUT}.py")
        endif()
        message("python3 -m grpc_tools.protoc -I${PROTOBUF_INPUT_DIRECTORY} --python_out=${GENERATED_TESTS_DIR} --grpc_python_out=${GENERATED_TESTS_DIR} --experimental_allow_proto3_optional ${CUR_PROTO_FILE}")
    endforeach(CUR_PROTO_FILE)
    add_definitions(-DWITH_GRPC)
endif()
file(GLOB GRPC_SRCS ./*.cc ${GENERATED_PROTO_DIR}/*.cc)

if (APPLE)
   set (CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo") 
   set (CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -gz")
endif ()

add_library(${TARGET} SHARED ${GRPC_SRCS})
include_directories(${GENERATED_PROTO_DIR})
target_link_libraries(${TARGET} ${GRPC_LIBRARIES})

set_target_properties(${TARGET} PROPERTIES LIBRARY_OUTPUT_DIRECTORY "../../cmd/reindexer_server" )

install(TARGETS ${TARGET}
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

