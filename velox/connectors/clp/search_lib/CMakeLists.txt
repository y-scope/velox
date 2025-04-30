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

add_subdirectory(clp_src/components/core/src/clp/string_utils)
set(YSTDLIB_CPP_BUILD_TESTING OFF)
add_subdirectory(clp_src/components/core/submodules/ystdlib-cpp
                 EXCLUDE_FROM_ALL)

string(LENGTH "${CMAKE_SOURCE_DIR}/" SOURCE_PATH_SIZE)
set(CLP_SRC_DIR clp_src/components/core/src)
antlr_target(
  KqlParser
  ${CLP_SRC_DIR}/clp_s/search/kql/Kql.g4
  LEXER
  PARSER
  VISITOR
  PACKAGE
  kql)

set(CLP_SRC_FILES
    ${ANTLR_KqlParser_CXX_OUTPUTS}
    "clp_src/components/core/submodules/date/include/date/date.h"
    ${CLP_SRC_DIR}/clp_s/archive_constants.hpp
    ${CLP_SRC_DIR}/clp_s/ArchiveReader.cpp
    ${CLP_SRC_DIR}/clp_s/ArchiveReader.hpp
    ${CLP_SRC_DIR}/clp_s/ArchiveReaderAdaptor.cpp
    ${CLP_SRC_DIR}/clp_s/ArchiveReaderAdaptor.hpp
    ${CLP_SRC_DIR}/clp_s/BufferViewReader.hpp
    ${CLP_SRC_DIR}/clp_s/ColumnReader.cpp
    ${CLP_SRC_DIR}/clp_s/ColumnReader.hpp
    ${CLP_SRC_DIR}/clp_s/ColumnReader.cpp
    ${CLP_SRC_DIR}/clp_s/ColumnWriter.hpp
    ${CLP_SRC_DIR}/clp_s/Compressor.hpp
    ${CLP_SRC_DIR}/clp_s/Decompressor.hpp
    ${CLP_SRC_DIR}/clp_s/Defs.hpp
    ${CLP_SRC_DIR}/clp_s/DictionaryEntry.cpp
    ${CLP_SRC_DIR}/clp_s/DictionaryEntry.hpp
    ${CLP_SRC_DIR}/clp_s/DictionaryReader.hpp
    ${CLP_SRC_DIR}/clp_s/DictionaryWriter.cpp
    ${CLP_SRC_DIR}/clp_s/DictionaryWriter.hpp
    ${CLP_SRC_DIR}/clp_s/ErrorCode.hpp
    ${CLP_SRC_DIR}/clp_s/FileReader.cpp
    ${CLP_SRC_DIR}/clp_s/FileReader.hpp
    ${CLP_SRC_DIR}/clp_s/FileWriter.cpp
    ${CLP_SRC_DIR}/clp_s/FileWriter.hpp
    ${CLP_SRC_DIR}/clp_s/InputConfig.cpp
    ${CLP_SRC_DIR}/clp_s/InputConfig.hpp
    ${CLP_SRC_DIR}/clp_s/JsonSerializer.hpp
    ${CLP_SRC_DIR}/clp_s/PackedStreamReader.cpp
    ${CLP_SRC_DIR}/clp_s/PackedStreamReader.hpp
    ${CLP_SRC_DIR}/clp_s/ParsedMessage.hpp
    ${CLP_SRC_DIR}/clp_s/ReaderUtils.cpp
    ${CLP_SRC_DIR}/clp_s/ReaderUtils.hpp
    ${CLP_SRC_DIR}/clp_s/Schema.cpp
    ${CLP_SRC_DIR}/clp_s/Schema.hpp
    ${CLP_SRC_DIR}/clp_s/SchemaMap.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaMap.hpp
    ${CLP_SRC_DIR}/clp_s/SchemaReader.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaReader.hpp
    ${CLP_SRC_DIR}/clp_s/SchemaTree.cpp
    ${CLP_SRC_DIR}/clp_s/SchemaTree.hpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryReader.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryReader.hpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryWriter.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampDictionaryWriter.hpp
    ${CLP_SRC_DIR}/clp_s/TimestampEntry.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampEntry.hpp
    ${CLP_SRC_DIR}/clp_s/TimestampPattern.cpp
    ${CLP_SRC_DIR}/clp_s/TimestampPattern.hpp
    ${CLP_SRC_DIR}/clp_s/TraceableException.hpp
    ${CLP_SRC_DIR}/clp_s/Utils.cpp
    ${CLP_SRC_DIR}/clp_s/Utils.hpp
    ${CLP_SRC_DIR}/clp_s/VariableEncoder.cpp
    ${CLP_SRC_DIR}/clp_s/VariableEncoder.hpp
    ${CLP_SRC_DIR}/clp_s/VariableDecoder.cpp
    ${CLP_SRC_DIR}/clp_s/VariableDecoder.hpp
    ${CLP_SRC_DIR}/clp_s/ZstdCompressor.cpp
    ${CLP_SRC_DIR}/clp_s/ZstdCompressor.hpp
    ${CLP_SRC_DIR}/clp_s/ZstdDecompressor.cpp
    ${CLP_SRC_DIR}/clp_s/ZstdDecompressor.hpp
    ${CLP_SRC_DIR}/clp_s/search/AddTimestampConditions.cpp
    ${CLP_SRC_DIR}/clp_s/search/AddTimestampConditions.hpp
    ${CLP_SRC_DIR}/clp_s/search/antlr_common/ErrorListener.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/AndExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/AndExpr.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/BooleanLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/BooleanLiteral.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ColumnDescriptor.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ColumnDescriptor.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConstantProp.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConstantProp.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConvertToExists.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/ConvertToExists.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/DateLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/DateLiteral.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/EmptyExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/EmptyExpr.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Expression.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Expression.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/FilterExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/FilterExpr.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/FilterOperation.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Integral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Integral.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Literal.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NarrowTypes.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NarrowTypes.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NullLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/NullLiteral.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrExpr.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrExpr.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrOfAndForm.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/OrOfAndForm.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/SearchUtils.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/SearchUtils.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/StringLiteral.cpp
    ${CLP_SRC_DIR}/clp_s/search/ast/StringLiteral.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Transformation.hpp
    ${CLP_SRC_DIR}/clp_s/search/ast/Value.hpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/EncodedVariableInterpreter.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/EncodedVariableInterpreter.hpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Grep.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Grep.hpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Query.cpp
    ${CLP_SRC_DIR}/clp_s/search/clp_search/Query.hpp
    ${CLP_SRC_DIR}/clp_s/search/EvaluateTimestampIndex.cpp
    ${CLP_SRC_DIR}/clp_s/search/EvaluateTimestampIndex.hpp
    ${CLP_SRC_DIR}/clp_s/search/kql/kql.cpp
    ${CLP_SRC_DIR}/clp_s/search/kql/kql.hpp
    ${CLP_SRC_DIR}/clp_s/search/Projection.cpp
    ${CLP_SRC_DIR}/clp_s/search/Projection.hpp
    ${CLP_SRC_DIR}/clp_s/search/QueryRunner.cpp
    ${CLP_SRC_DIR}/clp_s/search/QueryRunner.hpp
    ${CLP_SRC_DIR}/clp_s/search/SchemaMatch.cpp
    ${CLP_SRC_DIR}/clp_s/search/SchemaMatch.hpp
    ${CLP_SRC_DIR}/clp/aws/AwsAuthenticationSigner.cpp
    ${CLP_SRC_DIR}/clp/aws/AwsAuthenticationSigner.hpp
    ${CLP_SRC_DIR}/clp/BoundedReader.cpp
    ${CLP_SRC_DIR}/clp/BoundedReader.hpp
    ${CLP_SRC_DIR}/clp/CurlDownloadHandler.cpp
    ${CLP_SRC_DIR}/clp/CurlDownloadHandler.hpp
    ${CLP_SRC_DIR}/clp/CurlEasyHandle.hpp
    ${CLP_SRC_DIR}/clp/CurlGlobalInstance.cpp
    ${CLP_SRC_DIR}/clp/CurlGlobalInstance.hpp
    ${CLP_SRC_DIR}/clp/CurlOperationFailed.hpp
    ${CLP_SRC_DIR}/clp/CurlStringList.hpp
    ${CLP_SRC_DIR}/clp/Defs.h
    ${CLP_SRC_DIR}/clp/ErrorCode.hpp
    ${CLP_SRC_DIR}/clp/FileReader.cpp
    ${CLP_SRC_DIR}/clp/FileReader.hpp
    ${CLP_SRC_DIR}/clp/hash_utils.cpp
    ${CLP_SRC_DIR}/clp/hash_utils.hpp
    ${CLP_SRC_DIR}/clp/NetworkReader.cpp
    ${CLP_SRC_DIR}/clp/NetworkReader.hpp
    ${CLP_SRC_DIR}/clp/ReaderInterface.cpp
    ${CLP_SRC_DIR}/clp/ReaderInterface.hpp
    ${CLP_SRC_DIR}/clp/Thread.cpp
    ${CLP_SRC_DIR}/clp/Thread.hpp
    ${CLP_SRC_DIR}/clp/type_utils.hpp
    ${CLP_SRC_DIR}/clp/TraceableException.hpp)

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
  clp-s-search PUBLIC ${ANTLR_KqlParser_OUTPUT_DIR}
                      clp_src/components/core/submodules ${CLP_SRC_DIR})

velox_link_libraries(
  clp-s-search
  PRIVATE
    antlr4_static
    Boost::filesystem
    Boost::iostreams
    Boost::program_options
    Boost::url
    absl::flat_hash_map
    clp::string_utils
    ${CURL_LIBRARIES}
    glog::glog
    OpenSSL::Crypto
    simdjson::simdjson
    msgpack-cxx
    ystdlib::containers
    ystdlib::error_handling
    zstd::zstd
    spdlog::spdlog
    velox_vector
    velox_external_date)
