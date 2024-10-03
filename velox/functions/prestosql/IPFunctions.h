#pragma once

#include <folly/IPAddress.h>
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"

namespace facebook::velox::functions {

inline bool isIPV4(int128_t ip) {
  int128_t ipV4 = 0x0000FFFF00000000;
  uint128_t mask = 0xFFFFFFFFFFFFFFFF;
  mask = (mask << kIPV6HalfBits) | 0xFFFFFFFF00000000;
  return (ip & mask) == ipV4;
}

// Converts BigEndian <-> native byte array
// NOOP if system is Big Endian already
inline void bigEndianByteArray(folly::ByteArray16& addrBytes) {
  if (folly::kIsLittleEndian) {
    std::reverse(addrBytes.begin(), addrBytes.end());
  }
}

inline int128_t getIPFromIPPrefix(const facebook::velox::StringView ipPrefix) {
  int128_t result = 0;
  folly::ByteArray16 addrBytes;
  memcpy(&addrBytes, ipPrefix.data(), kIPAddressBytes);
  bigEndianByteArray(addrBytes);
  memcpy(&result, &addrBytes, kIPAddressBytes);
  return result;
}

template <typename T>
struct IPSubnetOfFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<IPPrefix>& ipPrefix,
      const arg_type<IPAddress>& ip) {
    uint128_t mask = 1;
    uint8_t prefix = ipPrefix.data()[kIPAddressBytes];
    int128_t checkIP = ip;

    if (isIPV4(getIPFromIPPrefix(ipPrefix))) {
      checkIP &= ((mask << (kIPV4Bits - prefix)) - 1) ^ -1;
    } else {
      // Special case: Overflow to all 0 subtracting 1 does not work.
      if (prefix == 0) {
        checkIP = 0;
      } else {
        checkIP &= ((mask << (kIPV6Bits - prefix)) - 1) ^ -1;
      }
    }
    result = (getIPFromIPPrefix(ipPrefix) == checkIP);
  }
};
} // namespace facebook::velox::functions
