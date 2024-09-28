#include "IpAddressType.h"

#include <boost/asio.hpp>

using namespace boost::asio;

namespace facebook::velox {

namespace {
class IpAddressCastOperator : public exec::CastOperator {
 public:
  bool isSupportedFromType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
  }

  bool isSupportedToType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
  }

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);

    if (input.typeKind() == TypeKind::VARCHAR) {
      castFromString(input, context, rows, *result);
    } else {
      VELOX_UNSUPPORTED(
          "Cast from {} to IPADDRESS not yet supported",
          resultType->toString());
    }
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);

    if (resultType->kind() == TypeKind::VARCHAR) {
      castToString(input, context, rows, *result);
    } else {
      VELOX_UNSUPPORTED(
          "Cast from IPADDRESS to {} not yet supported",
          resultType->toString());
    }
  }

 private:
  static void castToString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipAddresses = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipAddress = ipAddresses->valueAt(row);
      const auto ipAddressData = ipAddress.data();
      std::string ipAddressString;
      exec::StringWriter<false> result(flatResult, row);

      try {
        // Check if the input is an IPv4-mapped IPv6 address
        // (0:0:0:0:0:ffff:xxxx:xxxx)
        if (std::memcmp(ipAddressData, "\0\0\0\0\0\0\0\0\0\0\xff\xff", 12) ==
            0) {
          // Extract the IPv4 part
          ip::address_v4::bytes_type ipv4_bytes;
          std::memcpy(ipv4_bytes.data(), ipAddressData + 12, 4);
          ip::address_v4 ipv4_address(ipv4_bytes);
          ipAddressString = ipv4_address.to_string();
        } else {
          // Convert the entire data as IPv6
          ip::address_v6::bytes_type ipv6_bytes;
          std::memcpy(ipv6_bytes.data(), ipAddressData, 16);
          ip::address_v6 ipv6_address(ipv6_bytes);
          ipAddressString = ipv6_address.to_string();
        }
      } catch (const std::exception& e) {
        VELOX_USER_FAIL("Cannot cast the IP Address to a string: {}", e.what());
      }

      result.resize(ipAddressString.size());
      std::memcpy(
          result.data(), ipAddressString.data(), ipAddressString.size());
      result.finalize();
    });
  }

  static void castFromString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipAddressStrings = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipAddressString = ipAddressStrings->valueAt(row);
      exec::StringWriter<false> result(flatResult, row);
      result.resize(16);
      auto resultData = result.data();

      try {
        // Try to parse the input as an IP address (IPv4 or IPv6)
        ip::address address = ip::make_address(ipAddressString);

        // Convert the address to its binary form
        if (address.is_v4()) {
          std::memset(resultData, 0, 16);
          resultData[10] = static_cast<char>(0xff);
          resultData[11] = static_cast<char>(0xff);
          auto bytes = address.to_v4().to_bytes();
          std::memcpy(&resultData[12], bytes.data(), bytes.size());
        } else if (address.is_v6()) {
          auto bytes = address.to_v6().to_bytes();
          std::memcpy(resultData, bytes.data(), bytes.size());
        } else {
          VELOX_USER_FAIL("{} is not an IPv4 or IPv6 address", ipAddressString);
        }
      } catch (const std::exception& e) {
        VELOX_USER_FAIL(
            "Cannot cast the string to an IP Address: {}", e.what());
      }

      result.finalize();
    });
  }
};

class IpAddressTypeFactories : public CustomTypeFactories {
 public:
  IpAddressTypeFactories() = default;

  TypePtr getType() const override {
    return IPADDRESS();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<IpAddressCastOperator>();
  }
};
} // namespace

void registerIpAddressType() {
  registerCustomType(
      "ipaddress", std::make_unique<const IpAddressTypeFactories>());
}
} // namespace facebook::velox
