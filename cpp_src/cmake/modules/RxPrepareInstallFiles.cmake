# Prepare installation files and headers
if (NOT GO_BUILTIN_EXPORT_PKG_PATH)
  set (GO_BUILTIN_EXPORT_PKG_PATH "${PROJECT_SOURCE_DIR}/../bindings/builtin")
endif()

if (NOT GO_BUILTIN_SERVER_EXPORT_PKG_PATH)
  set (GO_BUILTIN_SERVER_EXPORT_PKG_PATH "${PROJECT_SOURCE_DIR}/../bindings/builtinserver")
endif()

if (GO_BUILTIN_EXPORT_PKG_PATH AND NOT IS_ABSOLUTE ${GO_BUILTIN_EXPORT_PKG_PATH})
  set (GO_BUILTIN_EXPORT_PKG_PATH "${CMAKE_CURRENT_SOURCE_DIR}/${GO_BUILTIN_EXPORT_PKG_PATH}")
endif()

  function(generate_libs_list INPUT_LIBS OUTPUT_LIST_NAME)
    set (flibs ${${OUTPUT_LIST_NAME}})
    foreach(lib ${INPUT_LIBS})
      get_filename_component(lib_dir ${lib} DIRECTORY)
      if (NOT ${lib_dir} STREQUAL "")
        set(lib_dir " -L${lib_dir}")
        if (NOT (${lib_dir} IN_LIST flibs))
          list(APPEND flibs ${lib_dir})
        endif()
      endif()
      if (${lib} MATCHES "jemalloc" OR ${lib} MATCHES "tcmalloc")
      elseif(${lib} STREQUAL "-pthread")
        list(APPEND flibs " -lpthread")
      elseif(${lib} MATCHES "^\\-.*")
        list(APPEND flibs " ${lib}")
      else()
        if (NOT "${lib}" STREQUAL "snappy" OR SNAPPY_FOUND)
          get_filename_component(lib_name ${lib} NAME_WE)
          string(REGEX REPLACE "^lib" "" lib_name ${lib_name})
          if (${lib} MATCHES "framework")
            list(APPEND flibs " -framework ${lib_name}")
          else()
            list(APPEND flibs " -l${lib_name}")
          endif()
        else()
          list(APPEND flibs " -l${lib}")
        endif()
      endif()
    endforeach(lib)
    set (${OUTPUT_LIST_NAME} ${flibs} PARENT_SCOPE)
  endfunction (generate_libs_list)


if (NOT WIN32)
  if (GO_BUILTIN_EXPORT_PKG_PATH AND EXISTS "${GO_BUILTIN_EXPORT_PKG_PATH}/posix_config.go.in")
    ProcessorCount (cgo_proc_count)
    set (cgo_cxx_flags "-I../../cpp_src ${EXTRA_FLAGS}")
    set (cgo_c_flags "-I../../cpp_src ${EXTRA_FLAGS}")
    set (cgo_ld_flags "-L\${SRCDIR}/../../build/cpp_src/ ${EXTRA_FLAGS}")
    generate_libs_list("${REINDEXER_LIBRARIES}" cgo_ld_flags)
    string(REPLACE ";" "" cgo_ld_flags "${cgo_ld_flags}")
    configure_file (
      "${GO_BUILTIN_EXPORT_PKG_PATH}/posix_config.go.in"
      "${GO_BUILTIN_EXPORT_PKG_PATH}/builtin_posix.go"
      @ONLY
    )
    unset (cgo_cxx_flags)
    unset (cgo_c_flags)
    unset (cgo_ld_flags)
  endif()

  SET(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "server")
  SET(DIST_INCLUDE_FILES
    "tools/errors.h" "tools/serializer.h" "tools/varint.h" "tools/stringstools.h" "tools/customhash.h" "tools/assertrx.h" "tools/jsonstring.h"
    "tools/verifying_updater.h" "tools/customlocal.h" "tools/clock.h" "tools/enum_compare.h" "tools/float_comparison.h"
    "core/reindexer.h" "core/type_consts.h" "core/type_formats.h" "core/item.h" "core/payload/payloadvalue.h" "core/payload/payloadiface.h" "core/indexopts.h"
    "core/namespacedef.h" "core/keyvalue/variant.h" "core/keyvalue/geometry.h" "core/sortingprioritiestable.h"
    "core/rdxcontext.h" "core/activity_context.h" "core/activity.h" "core/activitylog.h" "core/type_consts_helpers.h" "core/payload/fieldsset.h" "core/payload/payloadtype.h"
    "core/cbinding/reindexer_c.h" "core/cbinding/reindexer_ctypes.h" "core/transaction/transaction.h" "core/payload/payloadfieldtype.h" "core/reindexerconfig.h"
    "core/query/query.h" "core/query/queryentry.h" "core/queryresults/queryresults.h" "core/query/knn_search_params.h" "core/indexdef.h" "core/queryresults/aggregationresult.h"
    "core/queryresults/itemref.h" "core/namespace/stringsholder.h" "core/keyvalue/key_string.h" "core/keyvalue/uuid.h" "core/key_value_type.h"
    "core/namespace/incarnationtags.h" "core/keyvalue/p_string.h" "core/keyvalue/float_vector.h" "core/enums.h" "core/tag_name_index.h"
    "core/keyvalue/float_vectors_holder.h" "core/namespace/float_vectors_indexes.h" "core/keyvalue/float_vectors_keeper.h"
    "core/itemimplrawdata.h" "core/expressiontree.h" "tools/lsn.h" "core/cjson/tagspath.h" "core/cjson/ctag.h" "core/rank_t.h" "core/system_ns_names.h"
    "estl/cow.h" "core/shardedmeta.h" "estl/overloaded.h" "estl/concepts.h" "estl/types_pack.h"
    "core/queryresults/localqueryresults.h" "core/query/fields_names_filter.h" "core/queryresults/fields_filter.h"
    "estl/h_vector.h" "estl/marked_mutex.h" "estl/intrusive_ptr.h" "estl/trivial_reverse_iterator.h" "estl/chunk.h" "estl/expected.h" "estl/gift_str.h"
    "estl/thread_annotation_attributes.h" "estl/fast_hash_map.h" "vendor/hopscotch/hopscotch_map.h" "vendor/hopscotch/hopscotch_sc_map.h" 
    "vendor/hopscotch/hopscotch_hash.h" "estl/elist.h" "estl/mutex.h" "estl/forward_like.h"
    "estl/fast_hash_traits.h" "estl/debug_macros.h" "estl/defines.h" "estl/comparation_result.h" "estl/sparse_hash_int.h"
    "client/item.h" "client/resultserializer.h"
    "client/internalrdxcontext.h" "client/reindexer.h" "client/reindexerconfig.h"
    "client/cororeindexer.h" "client/coroqueryresults.h" "client/corotransaction.h" "client/connectopts.h"
    "client/queryresults.h" "client/transaction.h"
    "net/ev/ev.h" "vendor/koishi/include/koishi.h" "coroutine/coroutine.h" "coroutine/channel.h" "coroutine/waitgroup.h"
    "vendor/expected/expected.h"
    "debug/backtrace.h" "debug/allocdebug.h" "debug/resolver.h" "vendor/gason/gason.h"
  )

  foreach ( file ${DIST_INCLUDE_FILES} )
      get_filename_component( dir ${file} DIRECTORY )
      install( FILES ${PROJECT_SOURCE_DIR}/${file} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/reindexer/${dir} COMPONENT dev )
  endforeach()

  set (VERSION ${REINDEXER_VERSION})
  set (prefix ${CMAKE_INSTALL_PREFIX})
  set (exec_prefix ${CMAKE_INSTALL_FULL_BINDIR})
  set (libdir ${CMAKE_INSTALL_FULL_LIBDIR})
  set (includedir ${CMAKE_INSTALL_FULL_INCLUDEDIR}/reindexer)
  set (libs "")

  generate_libs_list("${REINDEXER_LIBRARIES}" libs)
  string(REPLACE ";" "" libs "${libs}")

  set(linkdirs "")
  foreach (lib ${REINDEXER_LIBRARIES})
    get_filename_component(lib ${lib} DIRECTORY)
    if (NOT ${lib} STREQUAL "")
      list(FIND linkdirs ${lib} index)
      if (NOT ${index} GREATER -1)
        list(APPEND linkdirs ${lib})
      endif()
    endif()
  endforeach(lib)

  configure_file (
    "${PROJECT_SOURCE_DIR}/libreindexer.pc.in"
    "${PROJECT_BINARY_DIR}/pkgconfig/libreindexer.pc"
    @ONLY
  )

  configure_file (
    "${PROJECT_SOURCE_DIR}/reindexer-config.cmake.in"
    "${PROJECT_BINARY_DIR}/pkgconfig/reindexer-config.cmake"
    @ONLY
  )

  configure_file (
    "${PROJECT_SOURCE_DIR}/reindexer-config-version.cmake.in"
    "${PROJECT_BINARY_DIR}/pkgconfig/reindexer-config-version.cmake"
    @ONLY
  )

  unset (VERSION)
  unset (prefix)
  unset (exec_prefix)
  unset (libdir)
  unset (includedir)
  unset (libs)
  unset (linkdirs)

  install(TARGETS ${TARGET}
      RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      COMPONENT dev
  )

  install(TARGETS friso_dict_resources
      COMPONENT dev
      RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
      LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
      ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  )

  install(DIRECTORY ${PROJECT_BINARY_DIR}/pkgconfig DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT dev)
  install(FILES ${PROJECT_BINARY_DIR}/pkgconfig/reindexer-config.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/reindexer COMPONENT dev)
  install(FILES ${PROJECT_BINARY_DIR}/pkgconfig/reindexer-config-version.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/reindexer COMPONENT dev)
else()
  if (GO_BUILTIN_EXPORT_PKG_PATH AND EXISTS "${GO_BUILTIN_EXPORT_PKG_PATH}/windows_config.go.in")
    set (cgo_ld_flags "-L\${SRCDIR}/../../build/cpp_src/ ${EXTRA_FLAGS}")
    generate_libs_list("${REINDEXER_LIBRARIES}" cgo_ld_flags)
    string(REPLACE ";" "" cgo_ld_flags "${cgo_ld_flags}")
    configure_file (
      "${GO_BUILTIN_EXPORT_PKG_PATH}/windows_config.go.in"
      "${GO_BUILTIN_EXPORT_PKG_PATH}/builtin_windows.go"
      @ONLY
    )
    unset (cgo_ld_flags)
  endif()
endif()
