cmake_minimum_required(VERSION 2.8)

project(documentation)

find_package(Doxygen)

if(DOXYGEN_FOUND)
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile)

    add_custom_target(doc
        ALL
        COMMAND ${DOXYGEN_EXECUTABLE} ${doxyfile}
        WORKING_DIRECTORY ${CMAKE_DOC_OUTPUT_DIRECTORY}
        COMMENT "Generating Reindexer documentation with Doxygen"
    )
endif()
