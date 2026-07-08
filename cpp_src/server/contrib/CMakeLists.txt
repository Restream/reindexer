cmake_minimum_required(VERSION 3.18)

include(CMakeParseArguments)

## Dependencies

find_program(python3 "python3")

function(check_python3_module ret)
    cmake_parse_arguments(
        ARGS
        ""
        "EXE" 
        "MODULES" 
        ${ARGN} )
    set(${ret} False PARENT_SCOPE) 
    if(ARGS_EXE)
        set(${ret} True PARENT_SCOPE) 
	foreach (m ${ARGS_MODULES})	
                execute_process(COMMAND ${ARGS_EXE} -c "import sys; from importlib import util; loader = util.find_spec('${m}'); sys.exit(loader is None)" RESULT_VARIABLE res)
		if(res EQUAL "1")
                        set(${ret} False PARENT_SCOPE) 
			message("python module ${m} not found")
			break()	
		endif()
	endforeach(m)
    endif(ARGS_EXE)
endfunction(check_python3_module)


if(python3)
    set(${PYTHON3_MODULE_EXISTS} False)
    check_python3_module(PYTHON3_MODULE_EXISTS EXE ${python3} MODULES yaml)
    if(PYTHON3_MODULE_EXISTS)
	    message("Add generate query.json.h")	
	    set(QUERY_SCHEMA "${PROJECT_SOURCE_DIR}/core/query/dsl/query.json.h")
	    add_custom_command(OUTPUT ${QUERY_SCHEMA}
                   COMMAND ${python3} dslshemagenerator.py ${CMAKE_BINARY_DIR}/query.json
                   DEPENDS ${PROJECT_SOURCE_DIR}/server/contrib/dslshemagenerator.py ${PROJECT_SOURCE_DIR}/server/contrib/server.yml
                   WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/server/contrib
                   COMMENT "Generate query.json.h"
                   )
            add_custom_target(query_json ALL DEPENDS ${QUERY_SCHEMA})
    endif()
endif()
