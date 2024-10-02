#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/IPFunctions.h"

namespace facebook::velox::functions {
void registerIpFunctions(const std::string& prefix) {
  registerIPPrefixType();
  registerIPAddressType();

  registerFunction<IPSubnetOfFunction, bool, IPPrefix , IPAddress>(
      {prefix + "is_subnet_of"});
}
} // namespace facebook::velox::functions
