add_subdirectory(search_lib)

string(LENGTH "${CMAKE_SOURCE_DIR}/" SOURCE_PATH_SIZE)
add_definitions("-DSOURCE_PATH_SIZE=${SOURCE_PATH_SIZE}")

velox_add_library(
  velox_clp_connector
  OBJECT
  ClpColumnHandle.cpp
  ClpConfig.cpp
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

velox_include_directories(velox_clp_connector PRIVATE)

target_compile_features(velox_clp_connector PRIVATE cxx_std_20)
