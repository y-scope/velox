ANTLR_TARGET(
        KqlParser
        Kql.g4
        LEXER PARSER VISITOR
        PACKAGE kql
)

# TODO Why do we need to add some of clp-s' cpp files here to allow presto_server to link
# correctly?
velox_add_library(
        kql
        STATIC
        ../../Utils.hpp
        ../AndExpr.hpp
        ../BooleanLiteral.cpp
        ../BooleanLiteral.hpp
        ../ColumnDescriptor.hpp
        ../DateLiteral.hpp
        ../DateLiteral.cpp
        ../EmptyExpr.hpp
        ../Expression.hpp
        ../FilterExpr.hpp
        ../Integral.hpp
        ../NullLiteral.hpp
        ../NullLiteral.cpp
        ../OrExpr.hpp
        ../StringLiteral.hpp
        ../StringLiteral.cpp
        ${ANTLR_KqlParser_CXX_OUTPUTS}
        kql.cpp
        kql.hpp
)

target_compile_features(kql PRIVATE cxx_std_20)
velox_include_directories(kql PRIVATE ${ANTLR_KqlParser_OUTPUT_DIR})
velox_link_libraries(kql PRIVATE absl::flat_hash_map antlr4_static Boost::filesystem)

