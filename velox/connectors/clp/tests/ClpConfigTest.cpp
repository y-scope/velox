/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdlib>
#include <unordered_map>

#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/clp/ClpConfig.h"
#include "velox/connectors/clp/search_lib/ClpPackageS3AuthProvider.h"

namespace facebook::velox::connector::clp {

namespace {

class ClpConfigTest : public testing::Test {
 public:
  std::unique_ptr<ClpConfig> buildClpConfig(
      std::unordered_map<std::string, std::string> configMap) {
    auto config =
        std::make_shared<const config::ConfigBase>(std::move(configMap));
    return std::make_unique<ClpConfig>(config);
  }
};

class ClpS3AuthProviderBaseTest : public ClpConfigTest {
 public:
  bool checkEnvironmentVariableEquals(
      std::string_view key,
      std::string_view value) {
    auto* actualValue = std::getenv(std::string(key).c_str());
    return 0 == std::strcmp(std::string(value).c_str(), actualValue);
  }

  bool checkEnvironmentVariableExists(std::string_view key) {
    auto* value = std::getenv(std::string(key).c_str());
    return value != nullptr;
  }
};

class ClpPackageS3AuthProviderTest : public ClpS3AuthProviderBaseTest {
 public:
  std::unique_ptr<ClpPackageS3AuthProvider> buildClpPackageS3AuthProvider(
      std::unordered_map<std::string, std::string> configMap) {
    auto config =
        std::make_shared<const config::ConfigBase>(std::move(configMap));
    return std::make_unique<ClpPackageS3AuthProvider>(config);
  }
};

} // namespace

TEST_F(ClpConfigTest, invalidAuthProvider) {
  const std::unordered_map<std::string, std::string> configMap(
      {{ClpConfig::kAuthProvider, "dummy-provider"}});
  // Both access/secret keys and iam-role cannot be specified
  VELOX_ASSERT_UNSUPPORTED_THROW(
      buildClpConfig(configMap),
      "Unsupported s3 auth provider type: dummy-provider.");
}

TEST_F(ClpS3AuthProviderBaseTest, caseInsensitiveAuthProvider) {
  const std::unordered_map<std::string, std::string> configMap(
      {{ClpConfig::kAuthProvider, "ClP_PaCkAgE"},
       {ClpPackageS3AuthProvider::kAccessKeyId, "aaaaaa"},
       {ClpPackageS3AuthProvider::kSecretAccessKey, "bbbbbb"}});
  VELOX_CHECK_NOT_NULL(buildClpConfig(configMap));
}

TEST_F(ClpPackageS3AuthProviderTest, readAndExportAwsAuthEnvironmentVariables) {
  const std::string cTestAccessKeyId{"aaaaaa"};
  const std::string cTestEndPoint{"http://aaaaaa"};
  const std::string cTestSecretAccessKey{"bbbbbb"};
  const std::string cTestSessionToken{"cccccc"};

  // Test all properties
  std::unordered_map<std::string, std::string> configMap(
      {{ClpConfig::kAuthProvider, "clp_package"},
       {ClpPackageS3AuthProvider::kAccessKeyId, cTestAccessKeyId},
       {ClpPackageS3AuthProvider::kEndPoint, cTestEndPoint},
       {ClpPackageS3AuthProvider::kSecretAccessKey, cTestSecretAccessKey},
       {ClpPackageS3AuthProvider::kSessionToken, cTestSessionToken}});
  auto clpPackageS3AuthProvider = buildClpPackageS3AuthProvider(configMap);
  VELOX_CHECK(clpPackageS3AuthProvider->exportAuthEnvironmentVariables());
  VELOX_CHECK(checkEnvironmentVariableEquals(
      ClpPackageS3AuthProvider::kEnvAwsAccessKeyId, cTestAccessKeyId));
  VELOX_CHECK(checkEnvironmentVariableEquals(
      ClpPackageS3AuthProvider::kEnvAwsSecretAccessKey, cTestSecretAccessKey));
  VELOX_CHECK(checkEnvironmentVariableEquals(
      ClpPackageS3AuthProvider::kEnvAwsSessionToken, cTestSessionToken));

  // Test auth without the session token
  configMap = {
      {ClpConfig::kAuthProvider, "clp_package"},
      {ClpPackageS3AuthProvider::kAccessKeyId, cTestAccessKeyId},
      {ClpPackageS3AuthProvider::kEndPoint, cTestEndPoint},
      {ClpPackageS3AuthProvider::kSecretAccessKey, cTestSecretAccessKey}};
  clpPackageS3AuthProvider = buildClpPackageS3AuthProvider(configMap);
  VELOX_CHECK(clpPackageS3AuthProvider->exportAuthEnvironmentVariables());
  VELOX_CHECK(
      false ==
      checkEnvironmentVariableExists(
          ClpPackageS3AuthProvider::kEnvAwsSessionToken));
}

} // namespace facebook::velox::connector::clp
