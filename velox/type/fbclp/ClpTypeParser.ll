%{
#include <vector>
#include <memory>

#include "velox/type/fbclp/ClpTypeParser.yy.h"  // @manual
#include "velox/type/fbclp/ClpScanner.h"
#define YY_DECL int facebook::velox::type::fbclp::ClpScanner::lex(facebook::velox::type::fbclp::ClpParser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault caseless

A   [A|a]
B   [B|b]
C   [C|c]
D   [D|d]
E   [E|e]
F   [F|f]
G   [G|g]
H   [H|h]
I   [I|i]
J   [J|j]
K   [K|k]
L   [L|l]
M   [M|m]
O   [O|o]
P   [P|p]
R   [R|r]
S   [S|s]
T   [T|t]
U   [U|u]
W   [W|w]
X   [X|x]
Y   [Y|y]
Z   [Z|z]

WORD              ([[:alpha:][:alnum:]_\-@#\$\\]*)
QUOTED_ID         (['"'][[:alnum:][:space:]_]*['"'])
NUMBER            ([[:digit:]]+)
VARIABLE          (VARCHAR|VARBINARY)

%%

"("                return ClpParser::token::LPAREN;
")"                return ClpParser::token::RPAREN;
","                return ClpParser::token::COMMA;
(ARRAY)            return ClpParser::token::ARRAY;
(MAP)              return ClpParser::token::MAP;
(FUNCTION)         return ClpParser::token::FUNCTION;
(DECIMAL)          return ClpParser::token::DECIMAL;
(ROW)              return ClpParser::token::ROW;
{VARIABLE}         yylval->build<std::string>(YYText()); return ClpParser::token::VARIABLE;
{NUMBER}           yylval->build<long long>(folly::to<int>(YYText())); return ClpParser::token::NUMBER;
{WORD}             yylval->build<std::string>(YYText()); return ClpParser::token::WORD;
{QUOTED_ID}        yylval->build<std::string>(YYText()); return ClpParser::token::QUOTED_ID;
<<EOF>>            return ClpParser::token::YYEOF;
.               /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "velox/type/fbclp/ClpTypeParser.h"

facebook::velox::TypePtr facebook::velox::type::fbclp::parseClpType(const std::string& typeText)
{
    std::istringstream is(typeText);
    std::ostringstream os;
    facebook::velox::TypePtr type;
    facebook::velox::type::fbclp::ClpScanner scanner{is, os, type, typeText};
    facebook::velox::type::fbclp::ClpParser parser{ &scanner };
    parser.parse();
    VELOX_CHECK(type, "Failed to parse type [{}]", typeText);
    return type;
}
