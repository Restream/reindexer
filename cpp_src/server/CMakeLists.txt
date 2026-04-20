cmake_minimum_required(VERSION 3.18)


project(reindexer_server_library)

set (SWAGGER_VERSION "5.32.2")
set (GH_FACE_VERSION "5.12.0")
set (GH_FACE_TAG "v${GH_FACE_VERSION}")
set (TARGET reindexer_server_library)
set (SERVER_LIB_DIR ${PROJECT_BINARY_DIR} PARENT_SCOPE)
file(WRITE ${PROJECT_BINARY_DIR}/swagger_replace.cmake
    "file(READ ${PROJECT_BINARY_DIR}/swagger/index.html indexhtml)
    STRING(REPLACE \"https://petstore.swagger.io/v2/swagger.json\" \"swagger.yml\" indexhtml \"\${indexhtml}\")
    file(WRITE ${PROJECT_BINARY_DIR}/swagger/index.html \"\${indexhtml}\")"
)
set(SWAGGER_UI_DIST_DIR
    ""
    CACHE PATH "Path to local swagger-ui-dist directory. If set and exists, local files are used; otherwise Swagger UI dist is downloaded.")
set(FACE_DIST_DIR
    ""
    CACHE PATH "Path to local face dist directory. If set and exists, local files are used; otherwise face dist is downloaded.")

file(WRITE ${PROJECT_BINARY_DIR}/make_swagger.cmake
  "cmake_policy(SET CMP0012 NEW)

  set(SWAGGER_UI_DIST_DIR \"${SWAGGER_UI_DIST_DIR}\")
  set(SWAGGER_VERSION \"${SWAGGER_VERSION}\")
  set(PROJECT_BINARY_DIR \"${PROJECT_BINARY_DIR}\")
  set(PROJECT_SOURCE_DIR \"${PROJECT_SOURCE_DIR}\")

  file(MAKE_DIRECTORY \"${PROJECT_BINARY_DIR}/swagger\")

  if(SWAGGER_UI_DIST_DIR AND EXISTS \"${SWAGGER_UI_DIST_DIR}\")
    execute_process(
      COMMAND \"${CMAKE_COMMAND}\" -E copy_directory
              \"${SWAGGER_UI_DIST_DIR}\"
              \"${PROJECT_BINARY_DIR}/swagger\"
      RESULT_VARIABLE ret
    )
    if(NOT \"\${ret}\" STREQUAL \"0\")
      message(FATAL_ERROR \"Could not copy local Swagger UI dist from '${SWAGGER_UI_DIST_DIR}'\")
    endif()
  elseif(SWAGGER_UI_DIST_DIR)
    message(FATAL_ERROR \"Local Swagger UI dist was not found at '${SWAGGER_UI_DIST_DIR}'\")
  else()
    file(
      DOWNLOAD
        \"https://github.com/swagger-api/swagger-ui/archive/refs/tags/v${SWAGGER_VERSION}.tar.gz\"
        \"${PROJECT_BINARY_DIR}/swagger.tar.gz\"
      STATUS download_status
      SHOW_PROGRESS
    )

    list(GET download_status 0 download_code)
    list(GET download_status 1 download_msg)
    if(NOT download_code EQUAL 0)
      message(FATAL_ERROR \"Could not download Swagger UI archive: \${download_msg}\")
    endif()

    execute_process(
      COMMAND \"${CMAKE_COMMAND}\" -E tar xzf \"${PROJECT_BINARY_DIR}/swagger.tar.gz\"
      WORKING_DIRECTORY \"${PROJECT_BINARY_DIR}\"
      RESULT_VARIABLE ret
    )
    if(NOT \"\${ret}\" STREQUAL \"0\")
      message(FATAL_ERROR \"Could not untar '${PROJECT_BINARY_DIR}/swagger.tar.gz'\")
    endif()

    execute_process(
      COMMAND \"${CMAKE_COMMAND}\" -E copy_directory
              \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist\"
              \"${PROJECT_BINARY_DIR}/swagger\"
      RESULT_VARIABLE ret
    )
    if(NOT \"\${ret}\" STREQUAL \"0\")
      message(FATAL_ERROR \"Could not copy directory '${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist'\")
    endif()

    execute_process(
      COMMAND \"${CMAKE_COMMAND}\" -E remove_directory
              \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}\"
      RESULT_VARIABLE ret
    )

    file(REMOVE \"${PROJECT_BINARY_DIR}/swagger.tar.gz\")
  endif()

  execute_process(
    COMMAND \"${CMAKE_COMMAND}\" -E copy
            \"${PROJECT_SOURCE_DIR}/contrib/server.yml\"
            \"${PROJECT_BINARY_DIR}/swagger/swagger.yml\"
    RESULT_VARIABLE ret
  )
  if(NOT \"\${ret}\" STREQUAL \"0\")
    message(FATAL_ERROR \"Could not copy '${PROJECT_SOURCE_DIR}/contrib/server.yml'\")
  endif()

  execute_process(
    COMMAND \"${CMAKE_COMMAND}\" -P \"${PROJECT_BINARY_DIR}/swagger_replace.cmake\"
    RESULT_VARIABLE ret
  )
  if(NOT \"\${ret}\" STREQUAL \"0\")
    message(FATAL_ERROR \"Could not patch swagger index.html\")
  endif()
")
file(WRITE ${PROJECT_BINARY_DIR}/make_face.cmake
    "set(FACE_DIST_DIR \"${FACE_DIST_DIR}\")
     set(PROJECT_BINARY_DIR \"${PROJECT_BINARY_DIR}\")
     execute_process(
	      COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${PROJECT_BINARY_DIR}/face\"
        RESULT_VARIABLE ret
     )
      if(FACE_DIST_DIR AND EXISTS \"${FACE_DIST_DIR}\")
       execute_process(
         COMMAND \"${CMAKE_COMMAND}\" -E copy_directory
                 \"${FACE_DIST_DIR}\"
                 \"${PROJECT_BINARY_DIR}/face\"
         RESULT_VARIABLE ret
       )
       if (NOT \"\${ret}\" STREQUAL \"0\")
         message(FATAL_ERROR \"Could not copy local face dist from '${FACE_DIST_DIR}'\")
       endif()
     elseif(FACE_DIST_DIR)
       message(FATAL_ERROR \"Local face dist was not found at '${FACE_DIST_DIR}'\")
     else()
       file(
         DOWNLOAD \"https://codeload.github.com/restream/reindexer-face-dist/tar.gz/${GH_FACE_TAG}\"
                  \"${PROJECT_BINARY_DIR}/face.tar.gz\"
       )
       execute_process(
         COMMAND \"${CMAKE_COMMAND}\" -E tar xzf \"face.tar.gz\" WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
         RESULT_VARIABLE ret
       )
       if (NOT \"\${ret}\" STREQUAL \"0\")
         message(FATAL_ERROR \"Could not untar 'face.tar.gz'\")
       endif()
       file(RENAME \"${PROJECT_BINARY_DIR}/reindexer-face-dist-${GH_FACE_VERSION}\" \"${PROJECT_BINARY_DIR}/face\")
       file(REMOVE \"${PROJECT_BINARY_DIR}/face.tar.gz\")
     endif()"
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
    set (cgo_ld_flags "-L\${SRCDIR}/../../build/cpp_src/ -L\${SRCDIR}/../../build/cpp_src/server/ -lreindexer_server_library ${EXTRA_FLAGS}")
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
