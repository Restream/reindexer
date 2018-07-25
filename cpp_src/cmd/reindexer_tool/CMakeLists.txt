cmake_minimum_required(VERSION 2.8)

project(reindexer_tool)

set(TARGET reindexer_tool)

find_library(ReplXX_LIBRARY NAMES ${ReplXX_NAMES} replxx)
find_path(ReplXX_INCLUDE_DIR NAMES replxx.hxx HINTS /opt/local/include /usr/local/include /usr/include)

if (NOT ReplXX_LIBRARY OR NOT ReplXX_INCLUDE_DIR)
  # replxx not found. Download it
  if (${CMAKE_MAJOR_VERSION} LESS 3)
    message (STATUS "ReplXX not found and can't built it: cmake version 3.0+ is required")
  else ()
    message (STATUS "ReplXX not found. Will download it")
    ExternalProject_Add(
      replxx_lib
      GIT_REPOSITORY "https://github.com/Restream/replxx"
      GIT_TAG "master"
      CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
    )
    include_directories (${CMAKE_CURRENT_BINARY_DIR}/include)
    link_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)
    list(APPEND REINDEXER_LIBRARIES replxx)
    add_definitions(-DREINDEX_WITH_REPLXX)
  endif ()


else ()
  message (STATUS "Found ReplXX: ${ReplXX_LIBRARY}")
  include_directories(${ReplXX_INCLUDE_DIR})
  list(APPEND REINDEXER_LIBRARIES ${ReplXX_LIBRARY})
  add_definitions(-DREINDEX_WITH_REPLXX)
endif ()


file(GLOB_RECURSE SRCS *.h *.cc)
add_executable(${TARGET} ${SRCS})

if (NOT ReplXX_LIBRARY OR NOT ReplXX_INCLUDE_DIR)
  add_dependencies(${TARGET} replxx_lib)
endif ()

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} )

install(TARGETS ${TARGET}
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
