#pragma once

#include "velox/expression/CastExpr.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {
class IpPrefixType : public VarbinaryType {
  IpPrefixType() = default;

 public:
  static const std::shared_ptr<const IpPrefixType>& get() {
    static const std::shared_ptr<const IpPrefixType> instance{
        new IpPrefixType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "IPPREFIX";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }
};

FOLLY_ALWAYS_INLINE bool isIpPrefixType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return IpPrefixType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const IpPrefixType> IPPREFIX() {
  return IpPrefixType::get();
}

struct IpPrefixT {
  using type = Varbinary;
  static constexpr const char* typeName = "ipprefix";
};

using IpPrefix = CustomType<IpPrefixT>;

void registerIpPrefixType();
} // namespace facebook::velox
