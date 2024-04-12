# Packaging and install stuff for the RPM/DEB/TGZ package
if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/issue")
  file(READ "/etc/issue" LINUX_ISSUE)
endif ()
if(CMAKE_SYSTEM_NAME MATCHES "Linux" AND EXISTS "/etc/os-release")
  file(READ "/etc/os-release" LINUX_ISSUE)
endif ()

set(CPACK_GENERATOR "TGZ")

if (WIN32) 
  set (CPACK_GENERATOR "NSIS")
elseif (LINUX_ISSUE MATCHES "Fedora" OR LINUX_ISSUE MATCHES "CentOS" OR LINUX_ISSUE MATCHES "Mandriva"
        OR LINUX_ISSUE MATCHES "RED OS")
  set(CPACK_GENERATOR "RPM")
  set(CPACK_PACKAGE_RELOCATABLE OFF)
elseif (LINUX_ISSUE MATCHES "altlinux")
  set(CPACK_GENERATOR "RPM")
  set(CPACK_PACKAGE_RELOCATABLE OFF)
  set(RPM_EXTRA_LIB_PREFIX "lib")
elseif (LINUX_ISSUE MATCHES "Ubuntu" OR LINUX_ISSUE MATCHES "Debian" OR LINUX_ISSUE MATCHES "Mint")
  set(CPACK_GENERATOR "DEB")
endif()

message ("Target cpack package type was detected as '${RxPrepareCpackDeps}'")

SET(CPACK_PACKAGE_NAME "reindexer")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "ReindexerDB server package")
SET(CPACK_PACKAGE_VENDOR "Reindexer")
SET(CPACK_PACKAGE_CONTACT "Reindexer team <contactus@reindexer.io>")
SET(CPACK_PACKAGE_VERSION ${REINDEXER_VERSION})

set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_DEB_COMPONENT_INSTALL ON)
if (WIN32)
  set(CPACK_SET_DESTDIR OFF)
else ()
  set(CPACK_SET_DESTDIR ON)
endif()

set(CPACK_RESOURCE_FILE_LICENSE ${REINDEXER_SOURCE_PATH}/../LICENSE)
set(CPACK_STRIP_FILES FALSE)

# Dependencies
set (CPACK_DEBIAN_PACKAGE_DEPENDS "")
set (CPACK_RPM_PACKAGE_REQUIRES_PRE "")

if (LevelDB_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libleveldb-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},${RPM_EXTRA_LIB_PREFIX}leveldb")
endif ()

if (RocksDB_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},librocksdb-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},${RPM_EXTRA_LIB_PREFIX}rocksdb")
endif ()

if (Z_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},zlib1g-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},zlib")
endif()

if (BZ2_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libbz2-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},bzip2")
endif()

if (LZ4_LIBRARY)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},liblz4-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},lz4")
endif()

if (SNAPPY_FOUND)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libsnappy-dev")
  SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},${RPM_EXTRA_LIB_PREFIX}snappy")
endif ()

if (LIBUNWIND)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libunwind-dev")
endif()

if (GPERFTOOLS_TCMALLOC)
  SET(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS},libgoogle-perftools4")
  if (LINUX_ISSUE MATCHES "altlinux")
    SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},gperftools")
  else ()
    SET(CPACK_RPM_PACKAGE_REQUIRES_PRE "${CPACK_RPM_PACKAGE_REQUIRES_PRE},gperftools-libs")
  endif ()
endif ()

# Remove first ',' from list of dependencies
if (CPACK_DEBIAN_PACKAGE_DEPENDS STREQUAL "")
  set (CPACK_DEBIAN_DEV_PACKAGE_DEPENDS "libleveldb-dev")
else ()
  string (SUBSTRING "${CPACK_DEBIAN_PACKAGE_DEPENDS}" 1 -1 CPACK_DEBIAN_PACKAGE_DEPENDS)
  set (CPACK_DEBIAN_DEV_PACKAGE_DEPENDS "libleveldb-dev,${CPACK_DEBIAN_PACKAGE_DEPENDS}")
endif ()

if (CPACK_RPM_PACKAGE_REQUIRES_PRE STREQUAL "")
  set (CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE "${RPM_EXTRA_LIB_PREFIX}leveldb-devel")
else()
  string (SUBSTRING "${CPACK_RPM_PACKAGE_REQUIRES_PRE}" 1 -1 CPACK_RPM_PACKAGE_REQUIRES_PRE)
  set (CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE "")
  string(REPLACE "," ";" CPACK_RPM_PACKAGE_REQUIRES_LIST "${CPACK_RPM_PACKAGE_REQUIRES_PRE}")
  foreach (DEP ${CPACK_RPM_PACKAGE_REQUIRES_LIST})
    if (NOT "${DEP}" STREQUAL "gperftools-libs")
      list(APPEND CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE "${DEP}-devel")
    else ()
      list(APPEND CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE "${DEP}")
    endif ()
  endforeach (DEP)
  string(REPLACE ";" "," CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE "${CPACK_RPM_DEV_PACKAGE_REQUIRES_PRE}")
endif ()

set (CPACK_DEBIAN_SERVER_FILE_NAME "DEB-DEFAULT")
set (CPACK_DEBIAN_DEV_FILE_NAME "DEB-DEFAULT")
set (CPACK_RPM_SERVER_FILE_NAME "RPM-DEFAULT")
set (CPACK_RPM_DEV_FILE_NAME "RPM-DEFAULT")

