cmake_minimum_required(VERSION 3.18)

project(documentation)

find_package(Doxygen)
find_program(Dox2html "dox2html5.py")
find_program(python3 "python3")

if(DOXYGEN_FOUND)
  if (Dox2html AND python3)
    configure_file(${PROJECT_SOURCE_DIR}/Doxyfile-mcss.in ${PROJECT_SOURCE_DIR}/Doxyfile-mcss)
    add_custom_target(doc
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      COMMAND ${python3} ${Dox2html} ${PROJECT_SOURCE_DIR}/Doxyfile-mcss
      COMMENT "Generating Reindexer documentation with Doxygen and Dox2html"
    )
  else()
    set(doxyfile ${PROJECT_SOURCE_DIR}/Doxyfile)
    add_custom_target(doc
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND sh -c "DOXYGEN_OUTPUT_DIRECTORY=${PROJECT_BINARY_DIR} ${DOXYGEN_EXECUTABLE} ${doxyfile}"
        COMMENT "Generating Reindexer documentation with Doxygen"
    )
  endif()
endif()
