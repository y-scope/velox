add_subdirectory(search_lib)

velox_add_library(
  velox_clp_connector
  OBJECT
  ClpConnector.cpp
  ClpDataSource.cpp
  ClpTableHandle.cpp)

velox_link_libraries(
  velox_clp_connector
  PRIVATE
    clp-s-search
    velox_connector
    simdjson::simdjson
    msgpack-cxx
    spdlog::spdlog
    absl::flat_hash_map
    Boost::filesystem)

target_compile_features(velox_clp_connector PRIVATE cxx_std_20)
