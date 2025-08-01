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
#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/clp/search_lib/ClpS3AuthProviderBase.h"

namespace facebook::velox::config {
class ConfigBase;
} // namespace facebook::velox::config

namespace facebook::velox::connector::clp {

void ClpS3AuthProviderBase::setupEnvironmentVariable(
    std::string_view key,
    std::string_view value) {
  int err{0};
#ifdef _WIN32
  // Windows version
  err = _putenv_s(std::string(key).c_str(), std::string(value).c_str());
#elif defined(__unix__) || defined(__APPLE__)
  // Unix/macOS version
  err = setenv(std::string(key).c_str(), std::string(value).c_str(), 1);
#else
  VELOX_UNSUPPORTED("Unsuppported OS");
#endif
  VELOX_CHECK_EQ(0, err);

  // Sanity check
  auto valueForCheck = std::getenv(std::string(key).c_str());
  VELOX_CHECK_EQ(0, std::strcmp(std::string(value).c_str(), valueForCheck));
}

void ClpS3AuthProviderBase::unsetEnvironmentVariable(std::string_view key) {
  int err{0};
#ifdef _WIN32
  // Windows version
  err = _putenv(fmt::format("{}=", std::string(key).c_str()));
#elif defined(__unix__) || defined(__APPLE__)
  // Unix/macOS version
  err = unsetenv(std::string(key).c_str());
#else
  VELOX_UNSUPPORTED("Unsuppported OS");
#endif
  VELOX_CHECK_EQ(0, err);

  // Sanity check
  auto valueForCheck = std::getenv(std::string(key).c_str());
  VELOX_CHECK_NULL(valueForCheck);
}

} // namespace facebook::velox::connector::clp
