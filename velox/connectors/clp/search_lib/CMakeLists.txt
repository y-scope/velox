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

set(CLP_EXTERNAL_BINARY_DIR ${CMAKE_BINARY_DIR}/external/clp)
add_subdirectory(${clp_SOURCE_DIR}/components/core/src/clp/string_utils
                 ${CLP_EXTERNAL_BINARY_DIR}/string_utils)
#add_subdirectory(${clp_SOURCE_DIR}/components/core/src/clp_s/search/kql
#                 ${CLP_EXTERNAL_BINARY_DIR}/kql)

string(LENGTH "${CMAKE_SOURCE_DIR}/" SOURCE_PATH_SIZE)

set(ANTLR_KqlParser_OUTPUT_DIR ${CLP_SRC_DIR}/clp_s/search/kql/antlr_generated)
file(GLOB ANTLR_KqlParser_CXX_OUTPUTS
    "${ANTLR_KqlParser_OUTPUT_DIR}/*.cpp"
)

set(CLP_SRC_FILES
    ${ANTLR_KqlParser_CXX_OUTPUTS}
    ${CLP_SRC_DIR}/clp_s/ArchiveReader.cpp
    ${CLP_SRC_DIR}/clp_s/ArchiveReaderAdaptor.cpp
    ${CLP_SRC_DIR}/clp_s/ColumnReader.cpp
    ${CLP_SRC_DIR}/clp_s/DictionaryEntry.cpp
    ${CLP_SRC_DIR}/clp_s/DictionaryWriter.cpp
    ${CLP_SRC_DIR}/clp_s/FileReader.cpp
    ${CLP_SRC_DIR}/clp_s/FileWriter.cpp
    ${CLP_SRC_DIR}/clp_s/InputConfig.cpp
    ${CLP_SRC_DIR}/clp_s/PackedStreamReader.cpp
    ${CLP_SRC_DIR}/clp_s/ReaderUtils.cpp
    ${CLP_SRC_DIR}/clp_s/Schema.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaMap.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaReader.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaTree.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryReader.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryWriter.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampEntry.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampPattern.cpp
    ${CLP_SRC_DIR}/clp_s/Utils.cpp
    ${CLP_SRC_DIR}/clp_s/VariableEncoder.cpp
    ${CLP_SRC_DIR}/clp_s/VariableDecoder.cpp
    ${CLP_SRC_DIR}/clp_s/ZstdCompressor.cpp
    ${CLP_SRC_DIR}/clp_s/ZstdDecompressor.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/AndExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/BooleanLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ColumnDescriptor.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConstantProp.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConvertToExists.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/DateLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/EmptyExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Expression.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/FilterExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Integral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NarrowTypes.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NullLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrOfAndForm.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/SearchUtils.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/StringLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/EncodedVariableInterpreter.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Grep.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Query.cpp
    ${CLP_SRC_DIR}/clp_s/search/kql/kql.cpp
    ${CLP_SRC_DIR}/clp_s/search/EvaluateTimestampIndex.cpp
    ${CLP_SRC_DIR}/clp_s/search/Projection.cpp
    ${CLP_SRC_DIR}/clp_s/search/QueryRunner.cpp
    ${CLP_SRC_DIR}/clp_s/search/SchemaMatch.cpp
    ${CLP_SRC_DIR}/clp/aws/AwsAuthenticationSigner.cpp
    ${CLP_SRC_DIR}/clp/BoundedReader.cpp
    ${CLP_SRC_DIR}/clp/CurlDownloadHandler.cpp
    ${CLP_SRC_DIR}/clp/CurlGlobalInstance.cpp
    ${CLP_SRC_DIR}/clp/Defs.h
    ${CLP_SRC_DIR}/clp/FileReader.cpp
    ${CLP_SRC_DIR}/clp/hash_utils.cpp
    ${CLP_SRC_DIR}/clp/NetworkReader.cpp
    ${CLP_SRC_DIR}/clp/ReaderInterface.cpp
    ${CLP_SRC_DIR}/clp/Thread.cpp)

velox_add_library(
  clp-s-search
  STATIC
  ${CLP_SRC_FILES}
  ClpVectorLoader.cpp
  ClpVectorLoader.h
  ClpCursor.cpp
  ClpCursor.h
  ClpQueryRunner.cpp
  ClpQueryRunner.h)
target_compile_features(clp-s-search PRIVATE cxx_std_20)
target_compile_definitions(clp-s-search
                           PUBLIC SOURCE_PATH_SIZE=${SOURCE_PATH_SIZE})
velox_include_directories(
  clp-s-search
  PUBLIC ${ANTLR_KqlParser_OUTPUT_DIR}
         ${clp_SOURCE_DIR}/components/core/submodules 
         ${CLP_SRC_DIR})

velox_link_libraries(
  clp-s-search
  PUBLIC msgpack-cxx spdlog::spdlog
  PRIVATE
    absl::flat_hash_map
    antlr4_static
    Boost::filesystem
    Boost::iostreams
    Boost::program_options
    Boost::url
    clp::string_utils
    date::date
    ${CURL_LIBRARIES}
    glog::glog
    OpenSSL::Crypto
    simdjson::simdjson
    nlohmann_json::nlohmann_json
    velox_vector
    ystdlib::containers
    ystdlib::error_handling
    zstd::zstd)
