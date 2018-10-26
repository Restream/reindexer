cmake_minimum_required(VERSION 3.0)

option(LINK_RESOURCES "Link web resources as binary data" OFF)

project(reindexer_server_library)

set (SWAGGER_VERSION "2.x")
set (FACE_BRANCH "master")
set(TARGET reindexer_server_library)
set(SERVER_LIB_DIR ${PROJECT_BINARY_DIR} PARENT_SCOPE)

file(WRITE ${PROJECT_BINARY_DIR}/swagger_replace.cmake
    "file(READ ${PROJECT_BINARY_DIR}/swagger/index.html indexhtml)
    string(REPLACE \"http://petstore.swagger.io/v2/swagger.json\" \"swagger.yml\" indexhtml \"\${indexhtml}\")
    file(WRITE ${PROJECT_BINARY_DIR}/swagger/index.html \"\${indexhtml}\")"
)

file(WRITE ${PROJECT_BINARY_DIR}/make_swagger.cmake
    "file(
	DOWNLOAD \"https://codeload.github.com/swagger-api/swagger-ui/tar.gz/${SWAGGER_VERSION}\"
	\"${PROJECT_BINARY_DIR}/swagger.tar.gz\"
     )
     execute_process(
       COMMAND ${CMAKE_COMMAND} -E tar xzf \"swagger.tar.gz\" WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not untar 'swagger.tar.gz'\")
     endif()
     execute_process(
       COMMAND ${CMAKE_COMMAND} -E copy_directory \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist\" \"${PROJECT_BINARY_DIR}/swagger\"
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not copy directory '${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}/dist'\")
     endif()
     execute_process(
       COMMAND ${CMAKE_COMMAND} -E copy \"${PROJECT_SOURCE_DIR}/contrib/server.yml\" \"${PROJECT_BINARY_DIR}/swagger/swagger.yml\"
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not copy '${PROJECT_SOURCE_DIR}/contrib/server.yml'\")
     endif()
     file(RENAME \"${PROJECT_BINARY_DIR}/swagger/swagger-ui.min.js\" \"${PROJECT_BINARY_DIR}/swagger/swagger-ui.js\")
     execute_process(COMMAND ${CMAKE_COMMAND} -P \"${PROJECT_BINARY_DIR}/swagger_replace.cmake\")
     execute_process(
       COMMAND ${CMAKE_COMMAND} -E remove_directory \"${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}\"
       RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not remove directory '${PROJECT_BINARY_DIR}/swagger-ui-${SWAGGER_VERSION}'\")
     endif()
     file(REMOVE \"${PROJECT_BINARY_DIR}/swagger.tar.gz\")"
)

file(WRITE ${PROJECT_BINARY_DIR}/make_face.cmake
    "file(
	DOWNLOAD \"https://codeload.github.com/restream/reindexer-face-dist/tar.gz/${FACE_BRANCH}\"
	\"${PROJECT_BINARY_DIR}/face.tar.gz\"
     )
     execute_process(
	 COMMAND ${CMAKE_COMMAND} -E remove_directory \"${PROJECT_BINARY_DIR}/face\"
	 RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not remove directory '${PROJECT_BINARY_DIR}/face'\")
     endif()
     execute_process(
	 COMMAND ${CMAKE_COMMAND} -E tar xzf \"face.tar.gz\" WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
	 RESULT_VARIABLE ret
     )
     if (NOT \"\${ret}\" STREQUAL \"0\")
	 message(FATAL_ERROR \"Could not untar 'face.tar.gz'\")
     endif()
     file(RENAME \"${PROJECT_BINARY_DIR}/reindexer-face-dist-${FACE_BRANCH}\" \"${PROJECT_BINARY_DIR}/face\")
     file(REMOVE \"${PROJECT_BINARY_DIR}/face.tar.gz\")"
)

if (LINK_RESOURCES)
	include(CMakeRC)
	execute_process(
	    COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_face.cmake"
	    COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_swagger.cmake"
	    RESULT_VARIABLE ret
	    ERROR_VARIABLE err
	)
	if (NOT "${ret}" STREQUAL "0")
	    message(FATAL_ERROR "Could not prepare 'face' and 'swagger' files. Reason: ${err}")
	endif()

	file(GLOB_RECURSE FACE_FILES ${PROJECT_BINARY_DIR}/face/*)
	file(GLOB_RECURSE SWAGGER_FILES ${PROJECT_BINARY_DIR}/swagger/*)
	cmrc_add_resource_library(resources WHENCE ${PROJECT_BINARY_DIR} ${FACE_FILES})
	cmrc_add_resources(resources WHENCE ${PROJECT_BINARY_DIR} ${SWAGGER_FILES})

	add_definitions(-DLINK_RESOURCES)
	list(APPEND REINDEXER_LIBRARIES resources)
endif()

file(GLOB_RECURSE SRCS *.h *.cc)
add_library(${TARGET} STATIC ${SRCS})
include_directories(${PROJECT_SOURCE_DIR})

if (LINK_RESOURCES)
    add_dependencies(${TARGET} resources)
    target_link_libraries(${TARGET} resources)
    target_link_libraries(${TARGET} reindexer)
endif()

add_custom_target(face COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_face.cmake")
add_custom_target(swagger COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_swagger.cmake")

install(FILES 
  "${PROJECT_SOURCE_DIR}/server.h"
DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/reindexer/server COMPONENT dev)
install(FILES 
  "${PROJECT_SOURCE_DIR}/cbinding/server_c.h"
DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/reindexer/server/cbinding COMPONENT dev)

install(TARGETS ${TARGET}
    COMPONENT dev
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
