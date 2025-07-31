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

#pragma once

#include <string_view>

#include "velox/common/config/Config.h"

namespace facebook::velox::connector::clp {

class ClpS3AuthProviderBase {
 public:
  virtual ~ClpS3AuthProviderBase() = default;

  /// Export the three environment variables needed by CLP-s to system:
  ///   AWS_ENV_VAR_ACCESS_KEY_ID
  ///   AWS_ENV_VAR_SECRET_ACCESS_KEY
  ///   AWS_ENV_VAR_SESSION_TOKEN (optional)
  /// So that in runtime, CLP-s’s code can execute S3-related logic correctly.
  ///
  /// @param config The config options parsed from clp.properties. User can
  /// define customized config options and use then here.
  /// @return Did exportation succeed or not.
  virtual bool exportAuthEnvironmentVariables(std::shared_ptr<const config::ConfigBase> config) const = 0;

 protected:
  /// Set the environment variables for different OS, then get the environment
  /// variables to do the sanity check.
  ///
  /// @param key The environment variable name.
  /// @param value The environment variable value.
  /// @return Did exportation succeed or not.
  static void setupEnvironmentVariables(std::string_view key, std::string_view value);
};

} // namespace facebook::velox::connector::clp