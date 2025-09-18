cmake_minimum_required(VERSION 3.18)

project(reindexer_tool)

set(TARGET reindexer_tool)

if (WITH_CPPTRACE)
  list(APPEND REINDEXER_LIBRARIES cpptrace ${REINDEXER_LIBRARIES})
endif()

if (MSVC)
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -DEBUG")
endif()

if (NOT MSVC AND NOT WITH_STDLIB_DEBUG)
  find_library(ReplXX_LIBRARY NAMES ${ReplXX_NAMES} replxx)
  find_path(ReplXX_INCLUDE_DIR NAMES replxx.hxx HINTS /opt/local/include /usr/local/include /usr/include)

  if (NOT ReplXX_LIBRARY OR NOT ReplXX_INCLUDE_DIR)
    # replxx not found. Download it
    message(STATUS "ReplXX not found. Will download it")
    ExternalProject_Add(
      replxx_lib
      GIT_REPOSITORY "https://github.com/Restream/replxx"
      GIT_TAG "574d7aa2bcc76731aadd7b4a89d96ee1796c2639"
      CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR} -DREPLXX_BuildExamples=OFF
    )
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)
    link_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)
    list(APPEND REINDEXER_LIBRARIES replxx)
    add_definitions(-DREINDEX_WITH_REPLXX)
  else()
    message(STATUS "Found ReplXX: ${ReplXX_LIBRARY}")
    include_directories(${ReplXX_INCLUDE_DIR})
    list(APPEND REINDEXER_LIBRARIES ${ReplXX_LIBRARY})
    add_definitions(-DREINDEX_WITH_REPLXX)
  endif()
endif()

file(GLOB_RECURSE SRCS *.h *.cc)
add_executable(${TARGET} ${SRCS})

# Enable export to provide readable stacktraces
set_property(TARGET ${TARGET} PROPERTY ENABLE_EXPORTS 1)
set_property(TARGET ${TARGET} PROPERTY DEFINE_SYMBOL "")

if (NOT MSVC AND NOT WITH_STDLIB_DEBUG)
  if (NOT ReplXX_LIBRARY OR NOT ReplXX_INCLUDE_DIR)
    add_dependencies(${TARGET} replxx_lib)
  endif()
endif()

target_link_libraries(${TARGET} ${REINDEXER_LIBRARIES} )

install(TARGETS ${TARGET}
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

if (MSVC)
  install(FILES $<TARGET_PDB_FILE:${PROJECT_NAME}> DESTINATION ${CMAKE_INSTALL_BINDIR} OPTIONAL)
endif()

get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
list(REMOVE_ITEM CPACK_COMPONENTS_ALL "leveldb_lib" "replxx_lib" "snappy_lib")
