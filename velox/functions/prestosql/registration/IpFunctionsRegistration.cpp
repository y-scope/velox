#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/IpFunctions.h"

namespace facebook::velox::functions {
void registerIpFunctions(const std::string& prefix) {
  registerIpPrefixType();
  registerIpAddressType();

  registerFunction<IsSubnetOfFunction, bool, IpPrefix , IpAddress>(
      {prefix + "is_subnet_of"});
}
} // namespace facebook::velox::functions
