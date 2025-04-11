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

#include "velox/common/config/Config.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::connector::clp {
class ClpConfig {
 public:
  static constexpr const char* kPolymorphicTypeEnabled =
      "clp.polymorphic-type-enabled";
  static constexpr const char* kInputSource = "clp.input-source";
  static constexpr const char* kArchiveDir = "clp.archive-dir";
  static constexpr const char* kS3Bucket = "clp.s3-bucket";
  static constexpr const char* kS3KeyPrefix = "clp.s3-key-prefix";

  [[nodiscard]] bool polymorphicTypeEnabled() const {
    return config_->get<bool>(kPolymorphicTypeEnabled, false);
  }

  [[nodiscard]] std::string inputSource() const {
    return config_->get<std::string>(kInputSource, "");
  }

  [[nodiscard]] std::string archiveDir() const {
    return config_->get<std::string>(kArchiveDir, "");
  }

  [[nodiscard]] std::string s3Bucket() const {
    return config_->get<std::string>(kS3Bucket, "");
  }

  [[nodiscard]] std::string s3KeyPrefix() const {
    return config_->get<std::string>(kS3KeyPrefix, "");
  }

  explicit ClpConfig(std::shared_ptr<const config::ConfigBase> config) {
    VELOX_CHECK_NOT_NULL(config, "Config is null for CLP initialization");
    config_ = std::move(config);
  }

  [[nodiscard]] const std::shared_ptr<const config::ConfigBase>& config()
      const {
    return config_;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};
} // namespace facebook::velox::connector::clp
