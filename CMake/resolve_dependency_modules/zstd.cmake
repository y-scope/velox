# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include_guard(GLOBAL)

# Version you want to build
set(VELOX_ZSTD_BUILD_VERSION 1.4.8)
set(VELOX_ZSTD_BUILD_SHA256_CHECKSUM
    32478297ca1500211008d596276f5367c54198495cf677e9439f4791a4c69f24)
set(VELOX_ZSTD_SOURCE_URL
    "https://github.com/facebook/zstd/releases/download/v${VELOX_ZSTD_BUILD_VERSION}/zstd-${VELOX_ZSTD_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(ZSTD)

message(STATUS "Building zstd from source")

# Force static lib, keep build minimal
set(ZSTD_BUILD_STATIC
    ON
    CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED
    OFF
    CACHE BOOL "" FORCE)
set(ZSTD_BUILD_PROGRAMS
    OFF
    CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(ZSTD_LEGACY_SUPPORT
    OFF
    CACHE BOOL "" FORCE)

FetchContent_Declare(
  zstd
  URL ${VELOX_ZSTD_SOURCE_URL}
  URL_HASH ${VELOX_ZSTD_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE EXCLUDE_FROM_ALL SYSTEM)

FetchContent_MakeAvailable(zstd)
add_library(zstd::libzstd_static INTERFACE IMPORTED)
