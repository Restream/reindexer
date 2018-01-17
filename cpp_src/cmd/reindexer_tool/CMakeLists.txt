cmake_minimum_required(VERSION 2.8)

project(reindexer_tool)

set(TARGET reindexer_tool)

file(
	GLOB_RECURSE
	SRCS
		${PROJECT_SOURCE_DIR}/*.cc
        ${REINDEXER_SOURCE_PATH}/pprof/backtrace.cc
)

add_executable(${TARGET} ${SRCS})

target_link_libraries(
    ${TARGET}
        reindexer
        ${LevelDB_LIBRARY}
        ${SNAPPY_LIBRARIES}
        ${GPERFTOOLS_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
  )
  