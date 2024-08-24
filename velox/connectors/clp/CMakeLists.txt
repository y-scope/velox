velox_add_library(
        velox_clp_connector OBJECT
        ClpColumnHandle.cpp
        ClpConfig.cpp
        ClpConnector.cpp
        ClpDataSource.cpp
        ClpTableHandle.cpp
)

velox_link_libraries(
        velox_clp_connector
        velox_connector
        simdjson::simdjson
        Boost::filesystem
)