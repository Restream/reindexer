#
# Locate and configure the gRPC library
#
# Adds the following targets:
#
#  gRPC::grpc - gRPC library
#  gRPC::grpc++ - gRPC C++ library
#  gRPC::grpc++_reflection - gRPC C++ reflection library
#  gRPC::grpc_cpp_plugin - C++ generator plugin for Protocol Buffers
#


# Find gRPC include directory
find_path(GRPC_INCLUDE_DIR grpc/grpc.h)
mark_as_advanced(GRPC_INCLUDE_DIR)

# Find gRPC library
find_library(GRPC_LIBRARY NAMES grpc)
mark_as_advanced(GRPC_LIBRARY)
add_library(gRPC::grpc UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES "-lpthread;-ldl"
    IMPORTED_LOCATION ${GRPC_LIBRARY}
)

# Find gRPC C++ library
find_library(GRPC_GRPC++_LIBRARY NAMES grpc++)
mark_as_advanced(GRPC_GRPC++_LIBRARY)
add_library(gRPC::grpc++ UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc++ PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES gRPC::grpc
    IMPORTED_LOCATION ${GRPC_GRPC++_LIBRARY}
)

# Find gRPC C++ reflection library
find_library(GRPC_GRPC++_REFLECTION_LIBRARY NAMES grpc++_reflection)
mark_as_advanced(GRPC_GRPC++_REFLECTION_LIBRARY)
add_library(gRPC::grpc++_reflection UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc++_reflection PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES gRPC::grpc++
    IMPORTED_LOCATION ${GRPC_GRPC++_REFLECTION_LIBRARY}
)

# Find gRPC CPP generator
find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin)
mark_as_advanced(GRPC_CPP_PLUGIN)
add_executable(gRPC::grpc_cpp_plugin IMPORTED)
set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES
    IMPORTED_LOCATION ${GRPC_CPP_PLUGIN}
)

include(${CMAKE_ROOT}/Modules/FindPackageHandleStandardArgs.cmake)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(gRPC DEFAULT_MSG
    GRPC_LIBRARY GRPC_INCLUDE_DIR  GRPC_CPP_PLUGIN)
