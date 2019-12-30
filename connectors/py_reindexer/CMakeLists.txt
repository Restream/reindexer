cmake_minimum_required(VERSION 3.0)

project(py_reindexer)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(PY_MIN_VERSION 3.6)
find_package(PythonInterp ${PY_MIN_VERSION})
find_package(PythonLibs ${PY_MIN_VERSION})

if (PYTHONINTERP_FOUND AND PYTHONLIBS_FOUND AND NOT WIN32 AND NOT WITH_ASAN AND NOT WITH_TSAN)
    set(PY_PACKAGE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/pyreindexer)

    set(LIB_BUILTIN_NAME "rawpyreindexerb")
    set(LIB_CPROTO_NAME "rawpyreindexerc")
    set(LIBS_EXT ".so")

    set(LIBSRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib/src)
    set(RESOURCES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib/include)

    file(GLOB_RECURSE SRCS ${RESOURCES_DIR}/*.cc ${LIBSRC_DIR}/*.cc)

    include_directories(
        ${LIBSRC_DIR}
        ${RESOURCES_DIR}
        ${PYTHON_INCLUDE_DIRS}
        ${REINDEXER_SOURCE_PATH}
        ${REINDEXER_SOURCE_PATH}/vendor)
    link_directories(${REINDEXER_LINK_DIRECTORIES_GLOBAL})

    add_library(${LIB_BUILTIN_NAME} SHARED ${SRCS})
    add_library(${LIB_CPROTO_NAME} SHARED ${SRCS})

    add_dependencies(${LIB_BUILTIN_NAME} reindexer)
    add_dependencies(${LIB_CPROTO_NAME} reindexer)

    target_link_libraries(${LIB_BUILTIN_NAME} ${PYTHON_LIBRARIES} ${REINDEXER_LIBRARIES_GLOBAL})
    target_link_libraries(${LIB_CPROTO_NAME} ${PYTHON_LIBRARIES} ${REINDEXER_LIBRARIES_GLOBAL})
    target_compile_definitions(${LIB_CPROTO_NAME} PRIVATE PYREINDEXER_CPROTO=1)
    target_compile_definitions(${LIB_CPROTO_NAME} PRIVATE REINDEX_CORE_BUILD=1)
    target_compile_definitions(${LIB_BUILTIN_NAME} PRIVATE REINDEX_CORE_BUILD=1)

    set_target_properties(${LIB_BUILTIN_NAME} PROPERTIES SUFFIX ${LIBS_EXT})
    set_target_properties(${LIB_CPROTO_NAME} PROPERTIES SUFFIX ${LIBS_EXT})

    # python does not allow 'lib' prefix
    set_target_properties(${LIB_BUILTIN_NAME} PROPERTIES PREFIX "")
    set_target_properties(${LIB_CPROTO_NAME} PROPERTIES PREFIX "")

    add_test (NAME pyreindexer_test COMMAND "python3" "-m" "unittest" WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    set_tests_properties(pyreindexer_test PROPERTIES ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:${CMAKE_CURRENT_BINARY_DIR}")


endif()
