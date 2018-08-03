cmake_minimum_required(VERSION 2.8)

option(LINK_RESOURCES "Link web resources as binary data" OFF)

project(reindexer_server)

set (SWAGGER_VERSION "2.x")
set (FACE_BRANCH "master")

set(TARGET reindexer_server)

file(GLOB_RECURSE SRCS *.cc *.h)
add_executable(${TARGET} ${SRCS})
include_directories(${PROJECT_SOURCE_DIR})

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
    if (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 3.1)
	message(WARNING "For build reindexer server with embedded resources need cmake version 3.1 or greater. This target will be skipped")
    else()
	include(CMakeRC)
	execute_process(
	    COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_face.cmake"
	    COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_swagger.cmake"
	)

	file(GLOB_RECURSE FACE_FILES ${PROJECT_BINARY_DIR}/face/*)
	file(GLOB_RECURSE SWAGGER_FILES ${PROJECT_BINARY_DIR}/swagger/*)
	cmrc_add_resource_library(resources WHENCE ${PROJECT_BINARY_DIR} ${FACE_FILES})
	cmrc_add_resources(resources WHENCE ${PROJECT_BINARY_DIR} ${SWAGGER_FILES})

	add_definitions(-DLINK_RESOURCES)
	list(APPEND REINDEXER_LIBRARIES resources)
    endif()
endif()

add_custom_target(face COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_face.cmake")
add_custom_target(swagger COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/make_swagger.cmake")

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES})

install(TARGETS ${TARGET}
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

install(DIRECTORY ${PROJECT_BINARY_DIR}/swagger ${PROJECT_BINARY_DIR}/face
    DESTINATION share/reindexer/web OPTIONAL
)

set (DESTINATION_ETC_DIR etc)
if (NOT APPLE AND NOT WIN32)
  set (DESTINATION_ETC_DIR ${DESTDIR}/etc)
  install (CODE "configure_file(${PROJECT_SOURCE_DIR}/contrib/sysvinit.in ${PROJECT_BINARY_DIR}/contrib/sysvinit)")
  install (FILES ${PROJECT_BINARY_DIR}/contrib/sysvinit DESTINATION ${DESTINATION_ETC_DIR}/init.d RENAME reindexer PERMISSIONS OWNER_WRITE OWNER_EXECUTE OWNER_READ GROUP_READ WORLD_READ)
endif()


install (CODE "
if (NOT WIN32)
  set (REINDEXER_INSTALL_PREFIX \"${CMAKE_INSTALL_PREFIX}\")
endif()
configure_file(${PROJECT_SOURCE_DIR}/contrib/config.yml.in ${PROJECT_BINARY_DIR}/contrib/config.yml)
")
install (FILES ${PROJECT_BINARY_DIR}/contrib/config.yml DESTINATION ${DESTINATION_ETC_DIR} RENAME reindexer.conf.pkg PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)

SET(CPACK_PACKAGE_NAME "reindexer_server")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "ReindexerDB server package")
SET(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${PROJECT_SOURCE_DIR}/contrib/deb/postinst)
SET(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${PROJECT_SOURCE_DIR}/contrib/rpm/postinst)

if (WIN32)
  if (MSVC)
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_SKIP true)
    include(InstallRequiredSystemLibraries)
    install(FILES ${CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS} DESTINATION ${CMAKE_INSTALL_BINDIR})
  else()
    get_filename_component(MINGW_DLL_DIR ${CMAKE_CXX_COMPILER} PATH)
    install(FILES
      ${MINGW_DLL_DIR}/libstdc++-6.dll
      ${MINGW_DLL_DIR}/libwinpthread-1.dll
      DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    if(EXISTS ${MINGW_DLL_DIR}/libgcc_s_seh-1.dll)
      install(FILES ${MINGW_DLL_DIR}/libgcc_s_seh-1.dll DESTINATION ${CMAKE_INSTALL_BINDIR})
    elseif(EXISTS ${MINGW_DLL_DIR}/libgcc_s_dw2-1.dll)
      install(FILES ${MINGW_DLL_DIR}/libgcc_s_dw2-1.dll DESTINATION ${CMAKE_INSTALL_BINDIR})
    else ()
      message (WARNING "Can't find MinGW runtime")
    endif()
  endif()

  SET (CPACK_NSIS_DEFINES
    "${CPACK_NSIS_DEFINES}
    !include ${PROJECT_SOURCE_DIR}\\\\contrib\\\\nsis\\\\strrep.nsh
    !include ${PROJECT_SOURCE_DIR}\\\\contrib\\\\nsis\\\\replaceinfile.nsh"
  )

  SET(CPACK_NSIS_EXTRA_PREINSTALL_COMMANDS "nsExec::Exec '\\\"$INSTDIR\\\\bin\\\\reindexer_server\\\" --remove'")
  SET(CPACK_NSIS_EXTRA_INSTALL_COMMANDS
    "${CPACK_NSIS_EXTRA_INSTALL_COMMANDS}
    CreateDirectory \\\"$APPDATA\\\\reindexer\\\"
    IfFileExists \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\" skip_patch_config do_patch_config
    do_patch_config:
    CopyFiles \\\"$INSTDIR\\\\etc\\\\reindexer.conf.pkg\\\" \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\"
    !insertmacro _ReplaceInFile \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\" \\\"/var/lib/reindexer\\\" \\\"$APPDATA\\\\reindexer\\\\db\\\"
    !insertmacro _ReplaceInFile \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\" \\\"/var/log/reindexer/\\\" \\\"$APPDATA\\\\reindexer\\\\log\\\\\\\"
    !insertmacro _ReplaceInFile \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\" \\\"/share/reindexer/\\\" \\\"$INSTDIR\\\\share\\\\reindexer\\\\\\\"
    skip_patch_config:
    CreateDirectory \\\"$APPDATA\\\\reindexer\\\\log\\\"
    CreateDirectory \\\"$APPDATA\\\\reindexer\\\\db\\\"
    nsExec::Exec 'Icacls \\\"$APPDATA\\\\reindexer\\\\*\\\" /grant \\\"(S-1-5-32-545)\\\"'
    nsExec::Exec '\\\"$INSTDIR\\\\bin\\\\reindexer_server\\\" --install --config \\\"$APPDATA\\\\reindexer\\\\reindexer.conf\\\"'
    nsExec::Exec 'net start reindexer'
  ")

  SET(CPACK_NSIS_EXTRA_UNINSTALL_COMMANDS "nsExec::Exec '\\\"$INSTDIR\\\\bin\\\\reindexer_server\\\" --remove'")
  SET(CPACK_NSIS_MENU_LINKS "http://127.0.0.1:9088/face" "Manage reindexer")
  SET(CPACK_NSIS_DISPLAY_NAME "Reindexer server")
  SET(CPACK_PACKAGE_INSTALL_DIRECTORY "reindexer")
endif (WIN32)

include(CPack)
