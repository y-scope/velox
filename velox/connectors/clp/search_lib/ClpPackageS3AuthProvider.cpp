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

#include "velox/connectors/clp/search_lib/ClpPackageS3AuthProvider.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

namespace facebook::velox::connector::clp {

namespace {
// Detects AWS S3 virtual-hosted style endpoints where bucket is in the hostname.
// Virtual-hosted: https://<bucket>.s3.<region>.amazonaws.com (returns true)
// Path-style: https://s3.<region>.amazonaws.com (returns false)
bool isAwsVirtualHostedStyleEndpoint(const std::string& endpoint) {
  if (endpoint.find("amazonaws.com") == std::string::npos) {
    return false;
  }

  auto schemeEnd = endpoint.find("://");
  if (schemeEnd == std::string::npos) {
    return false;
  }

  // Virtual-hosted has ".s3." in hostname (bucket.s3.region.amazonaws.com)
  // Path-style starts with "s3." (s3.region.amazonaws.com)
  auto hostPortion = endpoint.substr(schemeEnd + 3);
  return hostPortion.find(".s3.") != std::string::npos;
}
} // namespace

std::string ClpPackageS3AuthProvider::constructS3Url(
    std::string_view splitPath) {
  VELOX_CHECK(!splitPath.empty(), "splitPath cannot be empty");
  if (bucket_.empty()) {
    return fmt::format("{}/{}", endPoint_, splitPath);
  }
  return fmt::format("{}/{}/{}", endPoint_, bucket_, splitPath);
}

bool ClpPackageS3AuthProvider::exportAuthEnvironmentVariables() {
  endPoint_ = config_->get<std::string>(kEndPoint, "");
  VELOX_CHECK(!endPoint_.empty(), fmt::format("{} cannot be empty", kEndPoint));
  if ('/' == endPoint_.back()) {
    endPoint_.pop_back();
  }

  bucket_ = config_->get<std::string>(kBucket, "");
  VELOX_CHECK(
      bucket_.empty() || !isAwsVirtualHostedStyleEndpoint(endPoint_),
      "{} should not be set when using AWS S3 virtual-hosted style URLs. "
      "The bucket is already part of the endpoint hostname.",
      kBucket);

  auto accessKeyId = config_->get<std::string>(kAccessKeyId, "");
  auto secretAccessKey = config_->get<std::string>(kSecretAccessKey, "");
  auto sessionToken = config_->get<std::string>(kSessionToken, "");
  VELOX_CHECK(
      !accessKeyId.empty(), fmt::format("{} cannot be empty", kAccessKeyId));
  VELOX_CHECK(
      !secretAccessKey.empty(),
      fmt::format("{} cannot be empty", kSecretAccessKey));

  setEnvironmentVariable(kEnvAwsAccessKeyId, accessKeyId);
  setEnvironmentVariable(kEnvAwsSecretAccessKey, secretAccessKey);

  if (!sessionToken.empty()) {
    setEnvironmentVariable(kEnvAwsSessionToken, sessionToken);
  } else {
    unsetEnvironmentVariable(kEnvAwsSessionToken);
  }

  return true;
}

} // namespace facebook::velox::connector::clp
