cmake_minimum_required(VERSION 2.8)

project(reindexer_server)

set (SWAGGER_VERSION "2.x")
set (FACE_BRANCH "master")

set(TARGET reindexer_server)

file(GLOB_RECURSE SRCS *.cc *.h)
add_executable(${TARGET} ${SRCS})
include_directories(${PROJECT_SOURCE_DIR})
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES})

add_custom_target(swagger
    COMMAND rm -rf swagger/*
    COMMAND mkdir -p swagger
    COMMAND curl https://codeload.github.com/swagger-api/swagger-ui/tar.gz/${SWAGGER_VERSION} | tar xzv -C swagger swagger-ui-${SWAGGER_VERSION}/dist
    COMMAND mv -f "swagger/swagger-ui-${SWAGGER_VERSION}/dist/*" "swagger/"
    COMMAND rm -rf "swagger/swagger-ui-${SWAGGER_VERSION}" "swagger/swagger-ui.js"
    COMMAND sed -i_ "\"s/swagger-ui\\.js/swagger-ui\\.min\\.js/\"" "swagger/index.html"
    COMMAND sed -i_ "\"s/swagger\\.json/swagger\\.yml/\"" "swagger/index.html"
    COMMAND sed -i_ "\"s/http:\\/\\/petstore\\.swagger\\.io\\/v2\\///\"" "swagger/index.html"
    COMMAND sed -i_ "\"s/\\(ui = SwaggerUIBundle..\\)/\\1docExpansion:\\'none\\',/\"" "swagger/index.html"
    COMMAND rm -f "swagger/*_"
    COMMAND cp "${PROJECT_SOURCE_DIR}/contrib/server.yml" "swagger/swagger.yml"
)

add_custom_target(face
    COMMAND rm -rf face
    COMMAND curl -L https://github.com/restream/reindexer-face/-/jobs/artifacts/${FACE_BRANCH}/download?job=build -o /tmp/face.zip
    COMMAND unzip -o /tmp/face.zip
    COMMAND mv dist face
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
if (NOT APPLE)
  set (DESTINATION_ETC_DIR ${DESTDIR}/etc)
  install (CODE "configure_file(${PROJECT_SOURCE_DIR}/contrib/sysvinit.in ${PROJECT_BINARY_DIR}/contrib/sysvinit)")
  install (FILES ${PROJECT_BINARY_DIR}/contrib/sysvinit DESTINATION ${DESTINATION_ETC_DIR}/init.d RENAME reindexer PERMISSIONS OWNER_WRITE OWNER_EXECUTE OWNER_READ GROUP_READ WORLD_READ)
endif()

install (CODE "configure_file(${PROJECT_SOURCE_DIR}/contrib/config.yml.in ${PROJECT_BINARY_DIR}/contrib/config.yml)")
install (FILES ${PROJECT_BINARY_DIR}/contrib/config.yml DESTINATION ${DESTINATION_ETC_DIR} RENAME reindexer.conf.pkg PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)

SET(CPACK_PACKAGE_NAME "reindexer_server")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "ReindexerDB server package")
if (CPACK_GENERATOR EQUAL "DEB")
  SET(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${PROJECT_SOURCE_DIR}/contrib/deb/postinst)
elseif (CPACK_GENERATOR EQUAL "RPM")
  SET(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${PROJECT_SOURCE_DIR}/contrib/rpm/postinst)
endif()
include(CPack)
