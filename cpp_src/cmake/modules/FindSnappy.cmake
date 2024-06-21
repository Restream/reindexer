#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Tries to find Snappy headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Snappy)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  SNAPPY_ROOT_DIR  Set this variable to the root installation of
#                    Snappy if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  SNAPPY_FOUND              System has Snappy libs/headers
#  SNAPPY_LIBRARIES          The Snappy libraries
#  SNAPPY_INCLUDE_DIR        The location of Snappy headers

find_path(
    SNAPPY_INCLUDE_DIR 
        snappy.h
    HINTS
        ${SNAPPY_ROOT_DIR}/include
        /usr/include
        /opt/local/include
        /usr/local/include
)

set(SNAPPY_NAMES ${SNAPPY_NAMES} snappy)
find_library(
    SNAPPY_LIBRARY 
    NAMES 
        ${SNAPPY_NAMES} 
    HINTS
        ${SNAPPY_ROOT_DIR}/lib
        /usr/local/lib
        /opt/local/lib
        /usr/lib
)

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
  set(SNAPPY_FOUND TRUE)
  set( SNAPPY_LIBRARIES ${SNAPPY_LIBRARY} )
else()
  set(SNAPPY_FOUND FALSE)
  set( SNAPPY_LIBRARIES )
endif()

if (SNAPPY_FOUND)
  message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
else()
  message(STATUS "Not Found Snappy: ${SNAPPY_LIBRARY}")
  if (SNAPPY_FIND_REQUIRED)
    message(STATUS "Looked for Snappy libraries named ${SNAPPY_NAMES}.")
    message(FATAL_ERROR "Could NOT find Snappy library")
  endif()
endif()

mark_as_advanced(
  SNAPPY_LIBRARY
  SNAPPY_INCLUDE_DIR
)