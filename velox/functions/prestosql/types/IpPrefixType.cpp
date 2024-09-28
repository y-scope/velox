#include "IpPrefixType.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

using namespace boost::asio;

namespace facebook::velox {

namespace {
class IpPrefixCastOperator : public exec::CastOperator {
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
          "Cast from {} to IPPREFIX not yet supported", resultType->toString());
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
          "Cast from IPPREFIX to {} not yet supported", resultType->toString());
    }
  }

 private:
  static void castToString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipPrefixes = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipPrefix = ipPrefixes->valueAt(row);
      const auto ipPrefixData = ipPrefix.data();
      exec::StringWriter<false> result(flatResult, row);

      // Extract the subnet size
      uint8_t subnetSize;
      std::memcpy(&subnetSize, ipPrefix.data() + 16, 1);
      std::string ipPrefixString;
      try {
        // Check if it's an IPv4-mapped IPv6 address
        if (std::memcmp(ipPrefixData, "\0\0\0\0\0\0\0\0\0\0\xff\xff", 12) ==
            0) {
          // Convert to IPv4 address
          boost::asio::ip::address_v4::bytes_type ipv4_bytes;
          std::memcpy(ipv4_bytes.data(), ipPrefixData + 12, 4);
          ip::address_v4 ipv4_address(ipv4_bytes);
          ipPrefixString =
              ipv4_address.to_string() + "/" + std::to_string(subnetSize);
        } else {
          // Convert to IPv6 address
          boost::asio::ip::address_v6::bytes_type ipv6_bytes;
          std::memcpy(ipv6_bytes.data(), ipPrefixData, 16);
          ip::address_v6 ipv6_address(ipv6_bytes);
          ipPrefixString =
              ipv6_address.to_string() + "/" + std::to_string(subnetSize);
        }
      } catch (const std::exception& e) {
        VELOX_USER_FAIL("Cannot cast the IP Prefix to a string: {}", e.what());
      }

      result.resize(ipPrefixString.size());
      std::memcpy(result.data(), ipPrefixString.data(), ipPrefixString.size());
      result.finalize();
    });
  }

  static void castFromString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipPrefixStrings = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipPrefixString = ipPrefixStrings->valueAt(row);
      exec::StringWriter<false> result(flatResult, row);
      result.resize(17);
      auto resultData = result.data();

      std::vector<std::string> parts;
      boost::split(parts, ipPrefixString.getString(), boost::is_any_of("/"));
      if (parts.size() != 2) {
        VELOX_USER_FAIL(
            "Cannot cast the string  to an IP Prefix: {}", ipPrefixString);
      }

      // Parse the address
      ip::address address;
      try {
        address = ip::make_address(parts[0]);
      } catch (const std::exception& e) {
        VELOX_USER_FAIL("Cannot cast the string to an IP Prefix: {}", e.what());
      }
      auto subnetSize = static_cast<uint8_t>(std::stoi(parts[1]));

      if (address.is_v4()) {
        if (subnetSize < 0 || subnetSize > 32) {
          VELOX_USER_FAIL(
              "Cannot cast the string  to an IP Prefix: {}", ipPrefixString);
        }

        auto addrBytes = address.to_v4().to_bytes();
        // Apply subnet mask
        for (int i = 0; i < 4; i++) {
          addrBytes[3 - i] &=
              ~((1 << std::min(std::max(32 - subnetSize - 8 * i, 0), 8)) - 1);
        }
        // Copy IPv4-mapped address to IPv6 format
        std::memset(resultData, 0, 10);
        resultData[10] = static_cast<char>(0xff);
        resultData[11] = static_cast<char>(0xff);
        std::memcpy(resultData + 12, addrBytes.data(), 4);
        std::memcpy(resultData + 16, &subnetSize, 1);
      }
      // Handle IPv6 addresses
      else if (address.is_v6()) {
        if (subnetSize < 0 || subnetSize > 128) {
          VELOX_USER_FAIL(
              "Cannot cast the string to an IP Prefix: {}", ipPrefixString);
        }

        auto addrBytes = address.to_v6().to_bytes();
        // Apply subnet mask
        for (int i = 0; i < 16; i++) {
          addrBytes[15 - i] &=
              ~((1 << std::min(std::max(128 - subnetSize - 8 * i, 0), 8)) - 1);
        }
        std::memcpy(resultData, addrBytes.data(), 16);
        std::memcpy(resultData + 16, &subnetSize, 1);
      } else {
        VELOX_USER_FAIL(
            "Cannot cast the string to an IP Prefix: {}", ipPrefixString);
      }

      result.finalize();
    });
  }
};

class IpPrefixTypeFactories : public CustomTypeFactories {
 public:
  IpPrefixTypeFactories() = default;

  TypePtr getType() const override {
    return IPPREFIX();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<IpPrefixCastOperator>();
  }
};
} // namespace

void registerIpPrefixType() {
  registerCustomType(
      "ipprefix", std::make_unique<const IpPrefixTypeFactories>());
}
} // namespace facebook::velox
