#pragma once

#include "velox/common/config/Config.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::connector::clp {
class ClpConfig {
 public:
  static constexpr const char* kExecutablePath = "executable-path";
  static constexpr const char* kArchiveDir = "archive-dir";
  static constexpr const char* kPolymorphicTypeEnabled =
      "polymorphic-type-enabled";

  [[nodiscard]] std::string executablePath() const {
    return config_->get<std::string>(kExecutablePath, "");
  }

  [[nodiscard]] std::string archiveDir() const {
    return config_->get<std::string>(kArchiveDir, "");
  }

  [[nodiscard]] bool polymorphicTypeEnabled() const {
    return config_->get<bool>(kPolymorphicTypeEnabled, false);
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
