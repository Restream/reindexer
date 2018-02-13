cmake_minimum_required(VERSION 2.8)

project(reindexer_tool)

set(TARGET reindexer_tool)

file(GLOB_RECURSE SRCS *.h *.cc)
add_executable(${TARGET} ${SRCS})
target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES})

install(TARGETS ${TARGET}
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
