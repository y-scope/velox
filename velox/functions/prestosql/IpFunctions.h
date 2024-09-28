#pragma once

#include <boost/asio.hpp>

#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/types/IpAddressType.h"
#include "velox/functions/prestosql/types/IpPrefixType.h"

using namespace boost::asio;

namespace facebook::velox::functions {
template <typename T>
struct IsSubnetOfFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const arg_type<IpPrefix>& prefix,
      const arg_type<IpAddress>& address) {
    const auto prefixData = prefix.data();
    const auto addressData = address.data();
    // Extract the subnet size from the last byte of the prefix
    uint8_t subnetSize;
    std::memcpy(&subnetSize, prefixData + 16, 1);

    try {
      // Check if the IP prefix is an IPv4-mapped IPv6 address
      if (std::memcmp(prefixData, "\0\0\0\0\0\0\0\0\0\0\xff\xff", 12) == 0) {
        // IPv4 case: extract the IPv4 part
        ip::address_v4::bytes_type prefixBytes, targetBytes;
        std::memcpy(prefixBytes.data(), prefixData + 12, 4);
        std::memcpy(targetBytes.data(), addressData + 12, 4);

        // Create the IPv4 network and target address
        ip::network_v4 subnet =
            ip::make_network_v4(ip::address_v4(prefixBytes), subnetSize);
        ip::network_v4 targetNetwork =
            ip::make_network_v4(ip::address_v4(targetBytes), 32);

        // Check if the target address is in the subnet
        result = targetNetwork.is_subnet_of(subnet);
      } else {
        // IPv6 case
        ip::address_v6::bytes_type prefixBytes, targetBytes;
        std::memcpy(prefixBytes.data(), prefixData, 16);
        std::memcpy(targetBytes.data(), addressData, 16);

        // Create the IPv6 network and target address
        ip::network_v6 subnet =
            ip::make_network_v6(ip::address_v6(prefixBytes), subnetSize);
        ip::network_v6 targetNetwork =
            ip::make_network_v6(ip::address_v6(targetBytes), 128);

        // Check if the target address is in the subnet
        result = targetNetwork.is_subnet_of(subnet);
      }
    } catch (std::exception& e) {
      VELOX_USER_FAIL("Failed to parse IP prefix or IP address.");
    }
  }
};
} // namespace facebook::velox::functions
