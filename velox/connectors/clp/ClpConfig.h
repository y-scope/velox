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

  [[nodiscard]] const std::shared_ptr<const config::ConfigBase>& config() const {
    return config_;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};
} // namespace facebook::velox::connector::clp
