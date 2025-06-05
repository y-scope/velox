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
  ystdlib_cpp
  GIT_REPOSITORY https://github.com/y-scope/ystdlib-cpp.git
  GIT_TAG d80cf86e1a1f2dae6421978c8ee353408368f424
  GIT_SUBMODULES "" GIT_SUBMODULES_RECURSE TRUE)

FetchContent_MakeAvailable(ystdlib_cpp)

if(ystdlib_cpp_POPULATED)
  message(STATUS "Updating submodules for ystdlib-cpp...")
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E chdir "${ystdlib_cpp_SOURCE_DIR}" git submodule update
            --init --recursive
    RESULT_VARIABLE submodule_update_result
    OUTPUT_VARIABLE submodule_update_output
    ERROR_VARIABLE submodule_update_error)
  if(NOT ${submodule_update_result} EQUAL 0)
    message(ERROR
            "Failed to update submodules for ystdlib-cpp:\n${submodule_update_error}")
  else()
    message(STATUS "Submodules for ystdlib-cpp updated successfully.")
  endif()
endif()
