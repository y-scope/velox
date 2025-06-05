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
  clp
  GIT_REPOSITORY https://github.com/anlowee/clp.git
  GIT_TAG xwei/-remove-antlr
  GIT_SUBMODULES "" GIT_SUBMODULES_RECURSE TRUE)

FetchContent_Populate(clp)

add_subdirectory(${clp_SOURCE_DIR}/components/core/src/clp/string_utils
                 ${clp_BINARY_DIR}/string_utils)
