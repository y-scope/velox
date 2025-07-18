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

set(VELOX_DATE_BUILD_VERSION 3.0.1)
set(VELOX_DATE_BUILD_SHA256_CHECKSUM
    7a390f200f0ccd207e8cff6757e04817c1a0aec3e327b006b7eb451c57ee3538)
set(VELOX_DATE_SOURCE_URL
    "https://github.com/HowardHinnant/date/archive/refs/tags/v${VELOX_DATE_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(DATE)

# Optionally set CMake variables *before* make-available
set(CMAKE_INSTALL_MESSAGE
    LAZY
    CACHE STRING "" FORCE)

message(STATUS "Building date from source")

FetchContent_Declare(
  date
  URL ${VELOX_DATE_SOURCE_URL}
  URL_HASH ${VELOX_DATE_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE)

FetchContent_MakeAvailable(date)
