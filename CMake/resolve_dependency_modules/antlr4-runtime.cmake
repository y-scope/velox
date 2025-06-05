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

FetchContent_Declare(
  antlr4-runtime
  URL https://github.com/antlr/antlr4/archive/refs/tags/4.13.2.tar.gz
  URL_HASH SHA256=9f18272a9b32b622835a3365f850dd1063d60f5045fb1e12ce475ae6e18a35bb
)
FetchContent_Populate(antlr4-runtime)

# Set required flags before building
set(ANTLR4_INSTALL ON CACHE BOOL "" FORCE)

# Add only the C++ runtime
add_subdirectory(
  ${antlr4-runtime_SOURCE_DIR}/runtime/Cpp
  ${antlr4-runtime_BINARY_DIR}/runtime/Cpp
)
