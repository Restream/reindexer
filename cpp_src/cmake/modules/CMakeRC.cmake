if(_CMRC_GENERATE_MODE)
    # Read in the digits
    file(READ "${INPUT_FILE}" bytes HEX)
    # Format each pair into a character literal. Heuristics seem to favor doing
    # the conversion in groups of five for fastest conversion
    string(REGEX REPLACE "(..)(..)(..)(..)(..)" "'\\\\x\\1','\\\\x\\2','\\\\x\\3','\\\\x\\4','\\\\x\\5'," chars "${bytes}")
    # Since we did this in groups, we have some leftovers to clean up
    string(LENGTH "${bytes}" n_bytes2)
    math(EXPR n_bytes "${n_bytes2} / 2")
    math(EXPR remainder "${n_bytes} % 5") # <-- '5' is the grouping count from above
    set(cleanup_re "$")
    set(cleanup_sub )
    while(remainder)
	set(cleanup_re "(..)${cleanup_re}")
	set(cleanup_sub "'\\\\x\\${remainder}',${cleanup_sub}")
	math(EXPR remainder "${remainder} - 1")
    endwhile()
    if(NOT cleanup_re STREQUAL "$")
	string(REGEX REPLACE "${cleanup_re}" "${cleanup_sub}" chars "${chars}")
    endif()
    if (NOT "${bytes}" STREQUAL "")
	string(CONFIGURE [[
	namespace { const char file_array[] = { @chars@ }; }
	namespace cmrc { namespace @LIBRARY@ { namespace res_chars {
	  extern const char* const @SYMBOL@_begin = file_array;
	  extern const char* const @SYMBOL@_end = file_array + @n_bytes@;
	}}}
	]] code)
    else()
	string(CONFIGURE [[
	namespace { const char* file_array = nullptr; }
	namespace cmrc { namespace @LIBRARY@ { namespace res_chars {
	  extern const char* const @SYMBOL@_begin = file_array;
	  extern const char* const @SYMBOL@_end = file_array;
	}}}
	]] code)
    endif()
    file(WRITE "${OUTPUT_FILE}" "${code}")
    return()
endif()

if(COMMAND cmrc_add_resource_library)
    # CMakeRC has already been included! Don't do anything
    return()
endif()

set(this_script "${CMAKE_CURRENT_LIST_FILE}")

# CMakeRC uses std::call_once().
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

get_filename_component(_inc_dir "${CMAKE_BINARY_DIR}/_cmrc/include" ABSOLUTE)
set(CMRC_INCLUDE_DIR "${_inc_dir}" CACHE INTERNAL "Directory for CMakeRC include files")
# Let's generate the primary include file
file(MAKE_DIRECTORY "${CMRC_INCLUDE_DIR}/cmrc")
set(hpp_content [==[
#ifndef CMRC_CMRC_HPP_INCLUDED
#define CMRC_CMRC_HPP_INCLUDED

#include <string>
#include <map>
#include <mutex>

#define CMRC_INIT(libname) \
    do { \
	extern void cmrc_init_resources_##libname(); \
	cmrc_init_resources_##libname(); \
    } while (0)

namespace cmrc {

class resource {
    const char* _begin = nullptr;
    const char* _end = nullptr;
public:
    const char* begin() const { return _begin; }
    const char* end() const { return _end; }

    resource() = default;
    resource(const char* beg, const char* end) : _begin(beg), _end(end) {}
};

using resource_table = std::map<std::string, resource>;

namespace detail {

inline resource_table& table_instance() {
    static resource_table table;
    return table;
}

inline std::mutex& table_instance_mutex() {
    static std::mutex mut;
    return mut;
}

// We restrict access to the resource table through a mutex so that multiple
// threads can access it safely.
template <typename Func>
inline auto with_table(Func fn) -> decltype(fn(std::declval<resource_table&>())) {
    std::lock_guard<std::mutex> lk{ table_instance_mutex() };
    return fn(table_instance());
}

}

inline resource open(const char* fname) {
    return detail::with_table([fname](const resource_table& table) {
	auto iter = table.find(fname);
	if (iter == table.end()) {
	    return resource {};
	}
	return iter->second;
    });
}

inline resource open(const std::string& fname) {
    return open(fname.data());
}

}

#endif // CMRC_CMRC_HPP_INCLUDED
]==])

set(cmrc_hpp "${CMRC_INCLUDE_DIR}/cmrc/cmrc.hpp" CACHE INTERNAL "")
set(_generate 1)
if(EXISTS "${cmrc_hpp}")
    file(READ "${cmrc_hpp}" _current)
    if(_current STREQUAL hpp_content)
	set(_generate 0)
    endif()
endif()
file(GENERATE OUTPUT "${cmrc_hpp}" CONTENT "${hpp_content}" CONDITION ${_generate})

add_library(cmrc-base INTERFACE)
target_include_directories(cmrc-base INTERFACE "${CMRC_INCLUDE_DIR}")
target_compile_features(cmrc-base INTERFACE cxx_nullptr)
target_link_libraries(cmrc-base INTERFACE Threads::Threads)
set_property(TARGET cmrc-base PROPERTY INTERFACE_CXX_EXTENSIONS OFF)
add_library(cmrc::base ALIAS cmrc-base)

function(cmrc_add_resource_library name)
    # Generate the identifier for the resource library's namespace
    string(MAKE_C_IDENTIFIER "${name}" libident)
    # Generate a library with the compiled in character arrays.
    set(cpp_content [=[
	#include <cmrc/cmrc.hpp>
	#include <map>

	namespace cmrc { namespace %{libident} {

	namespace res_chars {
	// These are the files which are available in this resource library
	$<JOIN:$<TARGET_PROPERTY:%{libname},CMRC_EXTERN_DECLS>,
	>
	}

	inline void load_resources() {
	    // This initializes the list of resources and pointers to their data
	    static std::once_flag flag;
	    std::call_once(flag, [] {
		cmrc::detail::with_table([](resource_table& table) {
			(void)table;
		    $<JOIN:$<TARGET_PROPERTY:%{libname},CMRC_TABLE_POPULATE>,
		    >
		});
	    });
	}

	// namespace {
	//     extern struct resource_initializer {
	//         resource_initializer() {
	//             load_resources();
	//         }
	//     } dummy;
	// }

	}}

	// The resource library initialization function. Intended to be called
	// before anyone intends to use any of the resource defined by this
	// resource library
	extern void cmrc_init_resources_%{libident}() {
	    cmrc::%{libident}::load_resources();
	}
    ]=])
    get_filename_component(libdir "${CMAKE_CURRENT_BINARY_DIR}/${name}" ABSOLUTE)
    get_filename_component(lib_tmp_cpp "${libdir}/lib_.cpp" ABSOLUTE)
    string(REPLACE "%{libname}" "${name}" cpp_content "${cpp_content}")
    string(REPLACE "%{libident}" "${libident}" cpp_content "${cpp_content}")
    string(REPLACE "\n        " "\n" cpp_content "${cpp_content}")
    file(GENERATE OUTPUT "${lib_tmp_cpp}" CONTENT "${cpp_content}")
    get_filename_component(libcpp "${libdir}/lib.cpp" ABSOLUTE)
    add_custom_command(OUTPUT "${libcpp}"
	DEPENDS "${lib_tmp_cpp}" "${cmrc_hpp}"
	COMMAND ${CMAKE_COMMAND} -E copy_if_different "${lib_tmp_cpp}" "${libcpp}"
	COMMENT "Generating ${name} resource loader"
	)
    # Generate the actual static library. Each source file is just a single file
    # with a character array compiled in containing the contents of the
    # corresponding resource file.
    add_library(${name} STATIC ${libcpp})
    set_property(TARGET ${name} PROPERTY CMRC_LIBDIR "${libdir}")
    target_link_libraries(${name} PUBLIC cmrc::base)
    set_property(TARGET ${name} PROPERTY CMRC_IS_RESOURCE_LIBRARY TRUE)
    cmrc_add_resources(${name} ${ARGN})
endfunction()

function(cmrc_add_resources name)
    get_target_property(is_reslib ${name} CMRC_IS_RESOURCE_LIBRARY)
    if(NOT TARGET ${name} OR NOT is_reslib)
	message(SEND_ERROR "cmrc_add_resources called on target '${name}' which is not an existing resource library")
	return()
    endif()

    set(options)
    set(args WHENCE PREFIX)
    set(list_args)
    cmake_parse_arguments(PARSE_ARGV 1 ARG "${options}" "${args}" "${list_args}")

    if(NOT ARG_WHENCE)
	set(ARG_WHENCE ${CMAKE_CURRENT_SOURCE_DIR})
    endif()

    # Generate the identifier for the resource library's namespace
    string(MAKE_C_IDENTIFIER "${name}" libident)

    get_target_property(libdir ${name} CMRC_LIBDIR)

    foreach(input IN LISTS ARG_UNPARSED_ARGUMENTS)
	get_filename_component(abs_input "${input}" ABSOLUTE)
	# Generate a filename based on the input filename that we can put in
	# the intermediate directory.
	file(RELATIVE_PATH relpath "${ARG_WHENCE}" "${abs_input}")
	if(relpath MATCHES "^\\.\\.")
	    # For now we just error on files that exist outside of the soure dir.
	    message(SEND_ERROR "Cannot add file '${input}': File must be in a subdirectory of ${ARG_WHENCE}")
	    continue()
	endif()
	get_filename_component(abspath "${libdir}/intermediate/${relpath}.cpp" ABSOLUTE)
	# Generate a symbol name relpath the file's character array
	_cm_encode_fpath(sym "${relpath}")
	# Generate the rule for the intermediate source file
	_cmrc_generate_intermediate_cpp(${libident} ${sym} "${abspath}" "${abs_input}")
	target_sources(${name} PRIVATE ${abspath})
	set_property(TARGET ${name} APPEND PROPERTY CMRC_EXTERN_DECLS
	    "// Pointers to ${input}"
	    "extern const char* const ${sym}_begin\;"
	    "extern const char* const ${sym}_end\;"
	    )
	if(ARG_PREFIX AND NOT ARG_PREFIX MATCHES "/$")
	    set(ARG_PREFIX "${ARG_PREFIX}/")
	endif()
	set_property(TARGET ${name} APPEND PROPERTY CMRC_TABLE_POPULATE
	    "// Table entry for ${input}"
	    "table.emplace(\"${ARG_PREFIX}${relpath}\", resource{res_chars::${sym}_begin, res_chars::${sym}_end})\;"
	    )
    endforeach()
endfunction()

function(_cmrc_generate_intermediate_cpp libname symbol outfile infile)
    add_custom_command(
	# This is the file we will generate
	OUTPUT "${outfile}"
	# These are the primary files that affect the output
	DEPENDS "${infile}" "${this_script}"
	COMMAND
	    "${CMAKE_COMMAND}"
		-D_CMRC_GENERATE_MODE=TRUE
		-DLIBRARY=${libname}
		-DSYMBOL=${symbol}
		"-DINPUT_FILE=${infile}"
		"-DOUTPUT_FILE=${outfile}"
		-P "${this_script}"
	COMMENT "Generating intermediate file for ${infile}"
    )
endfunction()

function(_cm_encode_fpath var fpath)
    string(MAKE_C_IDENTIFIER "${fpath}" ident)
    string(MD5 hash "${fpath}")
    string(SUBSTRING "${hash}" 0 4 hash)
    set(${var} f_${hash}_${ident} PARENT_SCOPE)
endfunction()
