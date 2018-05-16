cmake_minimum_required(VERSION 2.8)

project(reindexer_server)

set (SWAGGER_VERSION "2.x")
set (FACE_BRANCH "master")

set(TARGET reindexer_server)

file(GLOB_RECURSE SRCS *.cc *.h)
add_executable(${TARGET} ${SRCS})
include_directories(${PROJECT_SOURCE_DIR})
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES})

file(WRITE ${PROJECT_BINARY_DIR}/swagger_replace.cmake
  "file(READ ${PROJECT_BINARY_DIR}/swagger/index.html indexhtml)
  string(REPLACE \"http://petstore.swagger.io/v2/swagger.json\" \"swagger.yml\" indexhtml \"\${indexhtml}\")
  file(WRITE ${PROJECT_BINARY_DIR}/swagger/index.html \"\${indexhtml}\")"
)

ExternalProject_Add (swagger
  URL "https://codeload.github.com/swagger-api/swagger-ui/tar.gz/${SWAGGER_VERSION}"
  DOWNLOAD_NAME "swagger.tar.gz"
  CONFIGURE_COMMAND "" 
  BUILD_COMMAND ""
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_directory "${PROJECT_BINARY_DIR}/swagger-prefix/src/swagger/dist" "${PROJECT_BINARY_DIR}/swagger"
  COMMAND ${CMAKE_COMMAND} -E copy "${PROJECT_SOURCE_DIR}/contrib/server.yml" "${PROJECT_BINARY_DIR}/swagger/swagger.yml"
  COMMAND ${CMAKE_COMMAND} -E rename "${PROJECT_BINARY_DIR}/swagger/swagger-ui.min.js" "${PROJECT_BINARY_DIR}/swagger/swagger-ui.js"
  COMMAND ${CMAKE_COMMAND} -P "${PROJECT_BINARY_DIR}/swagger_replace.cmake"
)

ExternalProject_Add (face
  URL "https://codeload.github.com/restream/reindexer-face-dist/tar.gz/${FACE_BRANCH}"
  DOWNLOAD_NAME "face.tar.gz"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_directory "${PROJECT_BINARY_DIR}/face-prefix/src/face" "${PROJECT_BINARY_DIR}/face"
)

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

if (NOT WIN32)
  set (REINDEXER_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
endif()

install (CODE "configure_file(${PROJECT_SOURCE_DIR}/contrib/config.yml.in ${PROJECT_BINARY_DIR}/contrib/config.yml)")
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
