#pragma once

#include "velox/expression/CastExpr.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {
// Only IPv4 is supported
class IpAddressType : public VarbinaryType {
  IpAddressType() = default;

 public:
  static const std::shared_ptr<const IpAddressType>& get() {
    static const std::shared_ptr<const IpAddressType> instance{
        new IpAddressType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "IPADDRESS";
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

FOLLY_ALWAYS_INLINE bool isIpAddressType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return IpAddressType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const IpAddressType> IPADDRESS() {
  return IpAddressType::get();
}

struct IpAddressT {
  using type = Varbinary;
  static constexpr const char* typeName = "ipaddress";
};

using IpAddress = CustomType<IpAddressT>;

void registerIpAddressType();
} // namespace facebook::velox
