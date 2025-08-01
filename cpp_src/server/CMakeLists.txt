cmake_minimum_required(VERSION 3.18)


project(reindexer_server_library)

set (SWAGGER_VERSION "3.x")
set (GH_FACE_VERSION "5.5.0")
set (GH_FACE_TAG "v${GH_FACE_VERSION}")
set (TARGET reindexer_server_library)
set (SERVER_LIB_DIR ${PROJECT_BINARY_DIR} PARENT_SCOPE)

file(WRITE ${PROJECT_BINARY_DIR}/swagger_replace.cmake
    "file(READ ${PROJECT_BINARY_DIR}/swagger/index.html indexhtml)
    STRING(REPLACE \"https://petstore.swagger.io/v2/swagger.json\" \"swagger.yml\" indexhtml \"\${indexhtml}\")
    file(WRITE ${PROJECT_BINARY_DIR}/swagger/index.html \"\${indexhtml}\")"
)

file(WRITE ${PROJECT_BINARY_DIR}/make_swagger.cmake
    "file(
	DOWNLOAD \"https://codeload.github.com/swagger-api/swagger-ui/tar.gz/${SWAGGER_VERSION}\"
	\"${PROJECT_BINARY_DIR}/swagger.tar.gz\"
     )
     execute_process(
       COMMAND \"${CMAKE_COMMAND}\" -E tar xzf \"swagger.tar.gz\" WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not untar 'swagger.tar.gz'\")
     endif()
     execute_process(
       COMMAND \"${CMAKE_COMMAND}\" -E copy_directory \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist\" \"${PROJECT_BINARY_DIR}/swagger\"
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not copy directory '${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist'\")
     endif()
     execute_process(
       COMMAND \"${CMAKE_COMMAND}\" -E copy \"${PROJECT_SOURCE_DIR}/contrib/server.yml\" \"${PROJECT_BINARY_DIR}/swagger/swagger.yml\"
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not copy '${PROJECT_SOURCE_DIR}/contrib/server.yml'\")
     endif()
     execute_process(COMMAND \"${CMAKE_COMMAND}\" -P \"${PROJECT_BINARY_DIR}/swagger_replace.cmake\")
     execute_process(
       COMMAND ${CMAKE_COMMAND} -E remove_directory \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}\"
       RESULT_VARIABLE ret
     )
     file(REMOVE \"${PROJECT_BINARY_DIR}/swagger.tar.gz\")"
)

file(WRITE ${PROJECT_BINARY_DIR}/make_face.cmake
    "file(
  DOWNLOAD \"https://codeload.github.com/restream/reindexer-face-dist/tar.gz/${GH_FACE_TAG}\"
	\"${PROJECT_BINARY_DIR}/face.tar.gz\"
     )
     execute_process(
	 COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${PROJECT_BINARY_DIR}/face\"
	 RESULT_VARIABLE ret
     )
     execute_process(
	 COMMAND \"${CMAKE_COMMAND}\" -E tar xzf \"face.tar.gz\" WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
	 RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not untar 'face.tar.gz'\")
     endif()
     file(RENAME \"${PROJECT_BINARY_DIR}/reindexer-face-dist-${GH_FACE_VERSION}\" \"${PROJECT_BINARY_DIR}/face\")
     file(REMOVE \"${PROJECT_BINARY_DIR}/face.tar.gz\")"
)

if (LINK_RESOURCES)
  include(CMakeRC)
  message("-- Downloading swagger html assets...")
	execute_process(
	    COMMAND "${CMAKE_COMMAND}" -P "${PROJECT_BINARY_DIR}/make_swagger.cmake"
	    RESULT_VARIABLE ret
	    ERROR_VARIABLE err
	)
	if (NOT "${ret}" STREQUAL "0")
            message(FATAL_ERROR "Could not prepare 'swagger' files. Reason: ${err}")
	endif()
  message("-- Downloading face html assets...")
	execute_process(
	    COMMAND "${CMAKE_COMMAND}" -P "${PROJECT_BINARY_DIR}/make_face.cmake"
	    RESULT_VARIABLE ret
	    ERROR_VARIABLE err
	)
	if (NOT "${ret}" STREQUAL "0")
		message(FATAL_ERROR "Could not prepare 'face' files. Reason: ${err}")
	endif()
  
	file(GLOB_RECURSE FACE_FILES ${PROJECT_BINARY_DIR}/face*/*)
	file(GLOB_RECURSE SWAGGER_FILES ${PROJECT_BINARY_DIR}/swagger/*)
	cmrc_add_resource_library(reindexer_server_resources WHENCE ${PROJECT_BINARY_DIR} ${FACE_FILES})
	cmrc_add_resources(reindexer_server_resources WHENCE ${PROJECT_BINARY_DIR} ${SWAGGER_FILES})

	add_definitions(-DLINK_RESOURCES)
	list(APPEND REINDEXER_LIBRARIES reindexer_server_resources)
	install(TARGETS reindexer_server_resources
		COMPONENT dev
		RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
		LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
		ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
	)
endif()


file(GLOB SRCS
  *.cc *.h
  pprof/*
  statscollect/*
  cbinding/*
  events/*
)

file(GLOB_RECURSE VNDR_SRCS
  vendor/*
)
list(APPEND REINDEXER_LIBRARIES m)
add_library(${TARGET} STATIC ${SRCS} ${VNDR_SRCS})
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/vendor)

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
    <variant>
    <thread>
    <chrono>
    <atomic>

    ${REINDEXER_SOURCE_PATH}/estl/h_vector.h
    ${REINDEXER_SOURCE_PATH}/core/reindexer.h
    ${REINDEXER_SOURCE_PATH}/core/query/query.h
    ${REINDEXER_SOURCE_PATH}/core/queryresults/queryresults.h
    ${REINDEXER_SOURCE_PATH}/core/item.h
  )

  target_precompile_headers(${TARGET} PRIVATE ${PREC_HDRS})
endif()

if (LINK_RESOURCES)
    add_dependencies(${TARGET} reindexer_server_resources)
    target_link_libraries(${TARGET} reindexer_server_resources)
    target_link_libraries(${TARGET} reindexer)
endif()
if (ENABLE_GRPC)
    add_dependencies(${TARGET} reindexer_grpc_library)
    add_subdirectory(grpc)
    add_definitions(-DWITH_GRPC)
endif()

add_custom_target(face COMMAND "${CMAKE_COMMAND}" -P "${PROJECT_BINARY_DIR}/make_face.cmake")
add_custom_target(swagger COMMAND "${CMAKE_COMMAND}" -P "${PROJECT_BINARY_DIR}/make_swagger.cmake")

if (NOT WIN32)
  if (GO_BUILTIN_SERVER_EXPORT_PKG_PATH AND NOT IS_ABSOLUTE ${GO_BUILTIN_SERVER_EXPORT_PKG_PATH})
    set (GO_BUILTIN_SERVER_EXPORT_PKG_PATH "${CMAKE_CURRENT_SOURCE_DIR}/${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}")
  endif()

  if (GO_BUILTIN_SERVER_EXPORT_PKG_PATH AND EXISTS "${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}/posix_config.go.in")
    ProcessorCount (cgo_proc_count)
    set (cgo_cxx_flags "-I../../cpp_src ${EXTRA_FLAGS}")
    set (cgo_c_flags "-I../../cpp_src ${EXTRA_FLAGS}")
    set (cgo_ld_flags "-L\${SRCDIR}/../../build/cpp_src/ -L\${SRCDIR}/../../build/cpp_src/server/ -L\${SRCDIR}/../../build/cpp_src/server/vendor/ -lreindexer_server_library ${EXTRA_FLAGS}")
    generate_libs_list("${REINDEXER_LIBRARIES}" cgo_ld_flags)
    string(REPLACE ";" "" cgo_ld_flags "${cgo_ld_flags}")
    configure_file (
      "${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}/posix_config.go.in"
      "${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}/builtinserver_posix.go"
      @ONLY
    )
    unset (cgo_cxx_flags)
    unset (cgo_c_flags)
    unset (cgo_ld_flags)
  endif()

  install(FILES
  "${PROJECT_SOURCE_DIR}/server.h"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/reindexer/server COMPONENT dev)
  install(FILES
  "${PROJECT_SOURCE_DIR}/cbinding/server_c.h"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/reindexer/server/cbinding COMPONENT dev)

  set (VERSION ${REINDEXER_VERSION})
  set (prefix ${CMAKE_INSTALL_PREFIX})
  set (exec_prefix ${CMAKE_INSTALL_FULL_BINDIR})
  set (libdir ${CMAKE_INSTALL_FULL_LIBDIR})
  set (includedir ${CMAKE_INSTALL_FULL_INCLUDEDIR}/reindexer)
  set (libs "")
  generate_libs_list("${REINDEXER_LIBRARIES}" libs)
  string(REPLACE ";" "" libs "${libs}")
  set (libs "-lreindexer_server_library ${libs}")

  configure_file (
    "${PROJECT_SOURCE_DIR}/libreindexer_server.pc.in"
    "${PROJECT_BINARY_DIR}/pkgconfig/libreindexer_server.pc"
    @ONLY
  )

  unset (VERSION)
  unset (prefix)
  unset (exec_prefix)
  unset (libdir)
  unset (includedir)
  unset (libs)

  install(TARGETS ${TARGET}
      COMPONENT dev
      RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
      LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
      ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  )
  install(DIRECTORY ${PROJECT_BINARY_DIR}/pkgconfig DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT dev)
else()
    if (GO_BUILTIN_EXPORT_PKG_PATH AND EXISTS "${GO_BUILTIN_EXPORT_PKG_PATH}/windows_config.go.in")
      set (cgo_ld_flags "-L\${SRCDIR}/../../build/cpp_src/ -lreindexer_server_library ${EXTRA_FLAGS}")
      generate_libs_list("${REINDEXER_LIBRARIES}" cgo_ld_flags)
      string(REPLACE ";" "" cgo_ld_flags "${cgo_ld_flags}")
      configure_file (
        "${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}/windows_config.go.in"
        "${GO_BUILTIN_SERVER_EXPORT_PKG_PATH}/builtinserver_windows.go"
        @ONLY
      )
      unset (cgo_ld_flags)
    endif()
endif()
