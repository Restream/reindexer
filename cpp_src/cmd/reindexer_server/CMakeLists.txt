cmake_minimum_required(VERSION 3.0)

project(reindexer_server)

set(TARGET reindexer_server)

file (GLOB_RECURSE SRCS "main.cc")
add_executable(${TARGET} ${SRCS})
list(APPEND REINDEXER_LIBRARIES reindexer_server_library reindexer ${REINDEXER_LIBRARIES})
include_directories(${PROJECT_SOURCE_DIR})
add_dependencies(${TARGET} reindexer_server_library)

if (APPLE)
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,@loader_path")
elseif (NOT WIN32)
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,\$ORIGIN")
endif ()

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES})

install(TARGETS ${TARGET}
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

if (NOT LINK_RESOURCES)
  install(DIRECTORY ${SERVER_LIB_DIR}/swagger ${SERVER_LIB_DIR}/face
      DESTINATION share/reindexer/web OPTIONAL
  )
endif()

set (DESTINATION_ETC_DIR etc)
if (NOT APPLE AND NOT WIN32)
  set (DESTINATION_ETC_DIR ${DESTDIR}/etc)
  install (CODE "configure_file(${PROJECT_SOURCE_DIR}/contrib/sysvinit.in ${PROJECT_BINARY_DIR}/contrib/sysvinit)")
  install (FILES ${PROJECT_BINARY_DIR}/contrib/sysvinit DESTINATION ${DESTINATION_ETC_DIR}/init.d RENAME reindexer PERMISSIONS OWNER_WRITE OWNER_EXECUTE OWNER_READ GROUP_READ WORLD_READ)
endif()

install (CODE "
if (NOT WIN32)
   if (APPLE)
      set (REINDEXER_INSTALL_PREFIX \"${CMAKE_INSTALL_PREFIX}\")
   else()
      set (REINDEXER_INSTALL_PREFIX \"\${CMAKE_INSTALL_PREFIX}\")
   endif ()
endif()
configure_file(${PROJECT_SOURCE_DIR}/contrib/config.yml.in ${PROJECT_BINARY_DIR}/contrib/config.yml)
")
install (FILES ${PROJECT_BINARY_DIR}/contrib/config.yml DESTINATION ${DESTINATION_ETC_DIR} RENAME reindexer.conf.pkg PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)

SET(CPACK_DEBIAN_SERVER_PACKAGE_CONTROL_EXTRA ${PROJECT_SOURCE_DIR}/contrib/deb/postinst)
SET(CPACK_RPM_SERVER_POST_INSTALL_SCRIPT_FILE ${PROJECT_SOURCE_DIR}/contrib/rpm/postinst)
set(CPACK_RPM_RELOCATION_PATHS "/etc")
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
