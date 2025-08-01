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

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/clp/search_lib/ClpPackageS3AuthProvider.h"

namespace facebook::velox::connector::clp {

const std::string ClpPackageS3AuthProvider::constructS3Url(
    std::string_view splitPath) {
  if (this->endPoint_.empty()) {
    this->endPoint_ = config_->get<std::string>(kEndPoint, "");
  }
  if ('/' == this->endPoint_.back()) {
    this->endPoint_.pop_back();
  }
  return fmt::format("{}/{}", this->endPoint_, splitPath);
}

bool ClpPackageS3AuthProvider::exportAuthEnvironmentVariables() const {
  auto accessKeyId = config_->get<std::string>(kAccessKeyId, "");
  auto secretAccessKey = config_->get<std::string>(kSecretAccessKey, "");
  auto sessionToken = config_->get<std::string>(kSessionToken, "");
  VELOX_CHECK(!accessKeyId.empty());
  VELOX_CHECK(!secretAccessKey.empty());
  LOG(INFO) << "Setting " << kEnvAwsAccessKeyId
            << " environment variable: " << accessKeyId;
  setupEnvironmentVariable(kEnvAwsAccessKeyId, accessKeyId);
  LOG(INFO) << "Setting " << kEnvAwsSecretAccessKey
            << " environment variable: " << secretAccessKey;
  setupEnvironmentVariable(kEnvAwsSecretAccessKey, secretAccessKey);
  if (!sessionToken.empty()) {
    LOG(INFO) << "Setting " << kEnvAwsSessionToken
              << " environment variable: " << sessionToken;
    setupEnvironmentVariable(kEnvAwsSessionToken, sessionToken);
  } else {
    LOG(INFO) << "Unsetting " << kEnvAwsSessionToken
              << " environment variable.";
    unsetEnvironmentVariable(kEnvAwsSessionToken);
  }

  return true;
}

} // namespace facebook::velox::connector::clp
