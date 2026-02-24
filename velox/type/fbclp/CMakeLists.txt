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

if(${VELOX_ENABLE_CLP_CONNECTOR})
  if(${VELOX_BUILD_TESTING})
    add_subdirectory(tests)
  endif()

  bison_target(
    ClpTypeParser ClpTypeParser.yy
    ${CMAKE_CURRENT_BINARY_DIR}/ClpTypeParser.yy.cc
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/ClpTypeParser.yy.h
    COMPILE_FLAGS "-Werror -Wno-deprecated")

  flex_target(
    ClpTypeParserScanner ClpTypeParser.ll
    ${CMAKE_CURRENT_BINARY_DIR}/ClpScanner.cpp
    COMPILE_FLAGS "-Cf --prefix=veloxtpclp")

  add_flex_bison_dependency(ClpTypeParserScanner ClpTypeParser)

  if(VELOX_MONO_LIBRARY)
    add_custom_target(
      velox_type_fbclp_parser_gen_src
      DEPENDS ${BISON_ClpTypeParser_OUTPUTS}
              ${FLEX_ClpTypeParserScanner_OUTPUTS})
    add_dependencies(velox velox_type_fbclp_parser_gen_src)
  endif()
  velox_add_library(velox_type_fbclp_parser ${BISON_ClpTypeParser_OUTPUTS}
                    ${FLEX_ClpTypeParserScanner_OUTPUTS} ClpParserUtil.cpp)
  velox_include_directories(velox_type_fbclp_parser
                            PRIVATE ${PROJECT_BINARY_DIR} ${FLEX_INCLUDE_DIRS})
  velox_link_libraries(velox_type_fbclp_parser velox_common_base)
endif()
