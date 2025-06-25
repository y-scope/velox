# Copyright (c) Facebook, Inc. and its affiliates.Add commentMore actions
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
        ystdlib_cpp
        GIT_REPOSITORY https://github.com/y-scope/ystdlib-cpp.git
        GIT_TAG d80cf86e1a1f2dae6421978c8ee353408368f424
        GIT_SUBMODULES "" GIT_SUBMODULES_RECURSE TRUE)

FetchContent_Populate(ystdlib_cpp)

set(CLP_YSTDLIB_SOURCE_DIRECTORY "${ystdlib_cpp_SOURCE_DIR}")
