# Checks that a built stub exports exactly one symbol. Invoked as a post-build step:
# cmake -DLIBRARY=<path> -DEXPECTED_SYMBOL=rx_embed_get_api_base -DNM=<nm|dumpbin> [-DTOOL=dumpbin] -P RxVerifyExports.cmake
#
# The guarantee rests on one build setting each time: a version script on ELF, an exported symbols list on Mach-O, a module definition file
# under MSVC. Nothing else would notice if one of them were dropped.

if (NOT EXISTS "${LIBRARY}")
  message(FATAL_ERROR "RxVerifyExports: ${LIBRARY} does not exist")
endif()

if (TOOL STREQUAL "dumpbin")
  # "<ordinal> <hint> <RVA> <name>" rows, between a header and a summary. Nothing else on the line looks like that.
  execute_process(COMMAND "${NM}" /NOLOGO /EXPORTS "${LIBRARY}"
    OUTPUT_VARIABLE DUMP_OUTPUT RESULT_VARIABLE DUMP_RESULT ERROR_VARIABLE DUMP_ERROR)
  if (NOT DUMP_RESULT EQUAL 0)
    message(FATAL_ERROR "RxVerifyExports: could not read the exports of ${LIBRARY}: ${DUMP_ERROR}")
  endif()
  string(REPLACE "\n" ";" DUMP_LINES "${DUMP_OUTPUT}")
  set(EXPORTED "")
  foreach (LINE ${DUMP_LINES})
    string(STRIP "${LINE}" LINE)
    if (LINE MATCHES "^[0-9]+[ \t]+[0-9A-Fa-f]+[ \t]+[0-9A-Fa-f]+[ \t]+([^ \t]+)$")
      list(APPEND EXPORTED "${CMAKE_MATCH_1}")
    endif()
  endforeach()
  list(REMOVE_DUPLICATES EXPORTED)
  list(LENGTH EXPORTED EXPORTED_COUNT)
  if (NOT EXPORTED_COUNT EQUAL 1)
    message(FATAL_ERROR
      "RxVerifyExports: ${LIBRARY} exports ${EXPORTED_COUNT} symbols, expected exactly one.\n"
      " found: ${EXPORTED}\n"
      " WINDOWS_EXPORT_ALL_SYMBOLS is on, or something else carries __declspec(dllexport).")
  endif()
  if (NOT EXPORTED STREQUAL "${EXPECTED_SYMBOL}")
    message(FATAL_ERROR "RxVerifyExports: ${LIBRARY} exports '${EXPORTED}', expected '${EXPECTED_SYMBOL}'")
  endif()
  return()
endif()

# The two nm formats put the symbol name at opposite ends of the line, so the parser below needs to know which one produced the output.
set(NM_FORMAT "posix")
execute_process(COMMAND "${NM}" --dynamic --extern-only --defined-only --format=posix "${LIBRARY}"
  OUTPUT_VARIABLE NM_OUTPUT RESULT_VARIABLE NM_RESULT ERROR_VARIABLE NM_ERROR)
if (NOT NM_RESULT EQUAL 0)
  # BSD nm, as shipped on macOS, spells the same request differently and prints "<address> <type> <name>".
  set(NM_FORMAT "bsd")
  execute_process(COMMAND "${NM}" -g -U "${LIBRARY}"
    OUTPUT_VARIABLE NM_OUTPUT RESULT_VARIABLE NM_RESULT ERROR_VARIABLE NM_ERROR)
endif()
if (NOT NM_RESULT EQUAL 0)
  message(FATAL_ERROR "RxVerifyExports: could not read the symbol table of ${LIBRARY}: ${NM_ERROR}")
endif()

string(REPLACE "\n" ";" NM_LINES "${NM_OUTPUT}")
set(EXPORTED "")
foreach (LINE ${NM_LINES})
  string(STRIP "${LINE}" LINE)
  if (LINE STREQUAL "")
    continue()
  endif()
  if (NM_FORMAT STREQUAL "posix")
    # "<name> <type> <value> <size>"
    string(REGEX MATCH "^[^ \t]+" NAME "${LINE}")
  else()
    # "<address> <type> <name>"
    string(REGEX MATCH "[^ \t]+$" NAME "${LINE}")
  endif()
  if (NAME STREQUAL "")
    continue()
  endif()
  # Artifacts every toolchain adds to a shared object. They are not part of the interface.
  if (NAME MATCHES "^(_?_bss_start|_?_edata|_?_end|_?_fini|_?_init|_?_TMC_END__|__bss_start__|_bss_end__|__bss_end__|__end__|_?_dso_handle)$")
    continue()
  endif()
  list(APPEND EXPORTED "${NAME}")
endforeach()

list(REMOVE_DUPLICATES EXPORTED)
list(LENGTH EXPORTED EXPORTED_COUNT)
if (NOT EXPORTED_COUNT EQUAL 1)
  message(FATAL_ERROR
    "RxVerifyExports: ${LIBRARY} exports ${EXPORTED_COUNT} symbols, expected exactly one.\n"
    " found: ${EXPORTED}\n"
    " The version script or the exported symbols list is not being applied.")
endif()
if (NOT EXPORTED STREQUAL "${EXPECTED_SYMBOL}" AND NOT EXPORTED STREQUAL "_${EXPECTED_SYMBOL}")
  message(FATAL_ERROR "RxVerifyExports: ${LIBRARY} exports '${EXPORTED}', expected '${EXPECTED_SYMBOL}'")
endif()
