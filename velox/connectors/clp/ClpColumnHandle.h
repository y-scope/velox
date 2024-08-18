#pragma once

#include "velox/connectors/Connector.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::connector::clp {
class ClpColumnHandle : public ColumnHandle {
 public:
  ClpColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const std::vector<common::Subfield>& requiredSubfields = {})
      : name_(name), type_(type), requiredSubfields_(requiredSubfields) {}

  const std::string& name() const {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

  const std::vector<common::Subfield>& requiredSubfields() const {
    return requiredSubfields_;
  }

 private:
  const std::string name_;
  const TypePtr type_;
  const std::vector<common::Subfield> requiredSubfields_;
};
} // namespace facebook::velox::connector::clp
