cmake_minimum_required(VERSION 2.8)

project(documentation)

find_package(Doxygen)

if(DOXYGEN_FOUND)
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile)
    add_custom_target(doc
        ALL
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMAND sh -c "DOXYGEN_OUTPUT_DIRECTORY=${PROJECT_BINARY_DIR} ${DOXYGEN_EXECUTABLE} ${doxyfile}"
        COMMENT "Generating Reindexer documentation with Doxygen"
    )
endif()
