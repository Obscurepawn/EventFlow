
// Generated from EventFlowSql.g4 by ANTLR 4.10

#include "EventFlowSqlParser.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct EventFlowSqlParserStaticData final {
  EventFlowSqlParserStaticData(std::vector<std::string> ruleNames,
                               std::vector<std::string> literalNames,
                               std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  EventFlowSqlParserStaticData(const EventFlowSqlParserStaticData&) = delete;
  EventFlowSqlParserStaticData(EventFlowSqlParserStaticData&&) = delete;
  EventFlowSqlParserStaticData& operator=(const EventFlowSqlParserStaticData&) = delete;
  EventFlowSqlParserStaticData& operator=(EventFlowSqlParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

std::once_flag eventflowsqlParserOnceFlag;
EventFlowSqlParserStaticData* eventflowsqlParserStaticData = nullptr;

void eventflowsqlParserInitialize() {
  assert(eventflowsqlParserStaticData == nullptr);
  auto staticData = std::make_unique<EventFlowSqlParserStaticData>(
      std::vector<std::string>{"statement",
                               "createTable",
                               "columnDefinition",
                               "dataType",
                               "identifierList",
                               "createView",
                               "selectQuery",
                               "selectItem",
                               "tableReference",
                               "joinClause",
                               "expression",
                               "orExpression",
                               "andExpression",
                               "notExpression",
                               "comparisonExpression",
                               "additiveExpression",
                               "multiplicativeExpression",
                               "unaryExpression",
                               "primaryExpression",
                               "identifier"},
      std::vector<std::string>{
          "",    "",    "",    "",    "", "",     "",    "",     "",    "",    "",    "",
          "",    "",    "",    "",    "", "",     "",    "",     "",    "",    "",    "",
          "",    "",    "",    "",    "", "",     "",    "",     "",    "",    "",    "",
          "",    "",    "",    "",    "", "",     "",    "",     "",    "",    "",    "",
          "",    "",    "",    "",    "", "",     "'('", "')'",  "','", "';'", "'.'", "'+'",
          "'-'", "'*'", "'/'", "'='", "", "'<='", "'<'", "'>='", "'>'"},
      std::vector<std::string>{"",
                               "K_CREATE",
                               "K_TABLE",
                               "K_MATERIALIZED",
                               "K_VIEW",
                               "K_AS",
                               "K_PRIMARY",
                               "K_KEY",
                               "K_PARTITION",
                               "K_BY",
                               "K_SELECT",
                               "K_FROM",
                               "K_INNER",
                               "K_JOIN",
                               "K_ON",
                               "K_WHERE",
                               "K_GROUP",
                               "K_NULL",
                               "K_NOT",
                               "K_BOOL",
                               "K_INT64",
                               "K_FLOAT64",
                               "K_STRING",
                               "K_TIMESTAMP_MICROS",
                               "K_FLOAT_VECTOR",
                               "K_TRUE",
                               "K_FALSE",
                               "K_CAST",
                               "K_AND",
                               "K_OR",
                               "K_IS",
                               "K_DISTINCT",
                               "K_HAVING",
                               "K_ORDER",
                               "K_LIMIT",
                               "K_OFFSET",
                               "K_UNION",
                               "K_WITH",
                               "K_RECURSIVE",
                               "K_OVER",
                               "K_WINDOW",
                               "K_LEFT",
                               "K_RIGHT",
                               "K_FULL",
                               "K_OUTER",
                               "K_CROSS",
                               "K_NATURAL",
                               "K_USING",
                               "K_INSERT",
                               "K_UPDATE",
                               "K_DELETE",
                               "K_DROP",
                               "K_ALTER",
                               "K_REPLACE",
                               "LPAREN",
                               "RPAREN",
                               "COMMA",
                               "SEMICOLON",
                               "DOT",
                               "PLUS",
                               "MINUS",
                               "STAR",
                               "SLASH",
                               "EQ",
                               "NEQ",
                               "LE",
                               "LT",
                               "GE",
                               "GT",
                               "NUMBER",
                               "STRING_LITERAL",
                               "QUOTED_IDENTIFIER",
                               "IDENTIFIER",
                               "LINE_COMMENT",
                               "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4,   1,   74,  261, 2,   0,   7,   0,   2,   1,   7,   1,   2,   2,   7,   2,   2,   3,   7,
      3,   2,   4,   7,   4,   2,   5,   7,   5,   2,   6,   7,   6,   2,   7,   7,   7,   2,   8,
      7,   8,   2,   9,   7,   9,   2,   10,  7,   10,  2,   11,  7,   11,  2,   12,  7,   12,  2,
      13,  7,   13,  2,   14,  7,   14,  2,   15,  7,   15,  2,   16,  7,   16,  2,   17,  7,   17,
      2,   18,  7,   18,  2,   19,  7,   19,  1,   0,   1,   0,   1,   0,   3,   0,   44,  8,   0,
      1,   0,   3,   0,   47,  8,   0,   1,   0,   1,   0,   1,   1,   1,   1,   1,   1,   1,   1,
      1,   1,   1,   1,   1,   1,   5,   1,   58,  8,   1,   10,  1,   12,  1,   61,  9,   1,   1,
      1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   5,   1,   70,  8,   1,   10,
      1,   12,  1,   73,  9,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
      1,   3,   1,   82,  8,   1,   1,   2,   1,   2,   1,   2,   1,   2,   1,   2,   3,   2,   89,
      8,   2,   1,   3,   1,   3,   1,   4,   1,   4,   1,   4,   5,   4,   96,  8,   4,   10,  4,
      12,  4,   99,  9,   4,   1,   5,   1,   5,   1,   5,   1,   5,   1,   5,   1,   5,   1,   5,
      1,   6,   1,   6,   1,   6,   1,   6,   5,   6,   112, 8,   6,   10,  6,   12,  6,   115, 9,
      6,   1,   6,   1,   6,   1,   6,   3,   6,   120, 8,   6,   1,   6,   1,   6,   3,   6,   124,
      8,   6,   1,   6,   1,   6,   1,   6,   1,   6,   1,   6,   5,   6,   131, 8,   6,   10,  6,
      12,  6,   134, 9,   6,   3,   6,   136, 8,   6,   1,   7,   1,   7,   1,   7,   3,   7,   141,
      8,   7,   1,   8,   1,   8,   3,   8,   145, 8,   8,   1,   8,   3,   8,   148, 8,   8,   1,
      9,   3,   9,   151, 8,   9,   1,   9,   1,   9,   1,   9,   1,   9,   1,   9,   1,   10,  1,
      10,  1,   11,  1,   11,  1,   11,  5,   11,  163, 8,   11,  10,  11,  12,  11,  166, 9,   11,
      1,   12,  1,   12,  1,   12,  5,   12,  171, 8,   12,  10,  12,  12,  12,  174, 9,   12,  1,
      13,  5,   13,  177, 8,   13,  10,  13,  12,  13,  180, 9,   13,  1,   13,  1,   13,  1,   14,
      1,   14,  1,   14,  1,   14,  1,   14,  3,   14,  189, 8,   14,  1,   14,  3,   14,  192, 8,
      14,  1,   15,  1,   15,  1,   15,  5,   15,  197, 8,   15,  10,  15,  12,  15,  200, 9,   15,
      1,   16,  1,   16,  1,   16,  5,   16,  205, 8,   16,  10,  16,  12,  16,  208, 9,   16,  1,
      17,  5,   17,  211, 8,   17,  10,  17,  12,  17,  214, 9,   17,  1,   17,  1,   17,  1,   18,
      1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,
      18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,  5,   18,  234, 8,   18,  10,  18,  12,
      18,  237, 9,   18,  3,   18,  239, 8,   18,  1,   18,  1,   18,  1,   18,  1,   18,  1,   18,
      1,   18,  3,   18,  247, 8,   18,  3,   18,  249, 8,   18,  1,   18,  1,   18,  1,   18,  1,
      18,  1,   18,  1,   18,  3,   18,  257, 8,   18,  1,   19,  1,   19,  1,   19,  0,   0,   20,
      0,   2,   4,   6,   8,   10,  12,  14,  16,  18,  20,  22,  24,  26,  28,  30,  32,  34,  36,
      38,  0,   5,   1,   0,   19,  24,  1,   0,   63,  68,  1,   0,   59,  60,  1,   0,   61,  62,
      1,   0,   71,  72,  280, 0,   43,  1,   0,   0,   0,   2,   50,  1,   0,   0,   0,   4,   83,
      1,   0,   0,   0,   6,   90,  1,   0,   0,   0,   8,   92,  1,   0,   0,   0,   10,  100, 1,
      0,   0,   0,   12,  107, 1,   0,   0,   0,   14,  137, 1,   0,   0,   0,   16,  142, 1,   0,
      0,   0,   18,  150, 1,   0,   0,   0,   20,  157, 1,   0,   0,   0,   22,  159, 1,   0,   0,
      0,   24,  167, 1,   0,   0,   0,   26,  178, 1,   0,   0,   0,   28,  183, 1,   0,   0,   0,
      30,  193, 1,   0,   0,   0,   32,  201, 1,   0,   0,   0,   34,  212, 1,   0,   0,   0,   36,
      256, 1,   0,   0,   0,   38,  258, 1,   0,   0,   0,   40,  44,  3,   2,   1,   0,   41,  44,
      3,   10,  5,   0,   42,  44,  3,   12,  6,   0,   43,  40,  1,   0,   0,   0,   43,  41,  1,
      0,   0,   0,   43,  42,  1,   0,   0,   0,   44,  46,  1,   0,   0,   0,   45,  47,  5,   57,
      0,   0,   46,  45,  1,   0,   0,   0,   46,  47,  1,   0,   0,   0,   47,  48,  1,   0,   0,
      0,   48,  49,  5,   0,   0,   1,   49,  1,   1,   0,   0,   0,   50,  51,  5,   1,   0,   0,
      51,  52,  5,   2,   0,   0,   52,  53,  3,   38,  19,  0,   53,  59,  5,   54,  0,   0,   54,
      55,  3,   4,   2,   0,   55,  56,  5,   56,  0,   0,   56,  58,  1,   0,   0,   0,   57,  54,
      1,   0,   0,   0,   58,  61,  1,   0,   0,   0,   59,  57,  1,   0,   0,   0,   59,  60,  1,
      0,   0,   0,   60,  62,  1,   0,   0,   0,   61,  59,  1,   0,   0,   0,   62,  63,  5,   6,
      0,   0,   63,  64,  5,   7,   0,   0,   64,  65,  5,   54,  0,   0,   65,  66,  3,   8,   4,
      0,   66,  71,  5,   55,  0,   0,   67,  68,  5,   56,  0,   0,   68,  70,  3,   4,   2,   0,
      69,  67,  1,   0,   0,   0,   70,  73,  1,   0,   0,   0,   71,  69,  1,   0,   0,   0,   71,
      72,  1,   0,   0,   0,   72,  74,  1,   0,   0,   0,   73,  71,  1,   0,   0,   0,   74,  81,
      5,   55,  0,   0,   75,  76,  5,   8,   0,   0,   76,  77,  5,   9,   0,   0,   77,  78,  5,
      54,  0,   0,   78,  79,  3,   8,   4,   0,   79,  80,  5,   55,  0,   0,   80,  82,  1,   0,
      0,   0,   81,  75,  1,   0,   0,   0,   81,  82,  1,   0,   0,   0,   82,  3,   1,   0,   0,
      0,   83,  84,  3,   38,  19,  0,   84,  88,  3,   6,   3,   0,   85,  89,  5,   17,  0,   0,
      86,  87,  5,   18,  0,   0,   87,  89,  5,   17,  0,   0,   88,  85,  1,   0,   0,   0,   88,
      86,  1,   0,   0,   0,   88,  89,  1,   0,   0,   0,   89,  5,   1,   0,   0,   0,   90,  91,
      7,   0,   0,   0,   91,  7,   1,   0,   0,   0,   92,  97,  3,   38,  19,  0,   93,  94,  5,
      56,  0,   0,   94,  96,  3,   38,  19,  0,   95,  93,  1,   0,   0,   0,   96,  99,  1,   0,
      0,   0,   97,  95,  1,   0,   0,   0,   97,  98,  1,   0,   0,   0,   98,  9,   1,   0,   0,
      0,   99,  97,  1,   0,   0,   0,   100, 101, 5,   1,   0,   0,   101, 102, 5,   3,   0,   0,
      102, 103, 5,   4,   0,   0,   103, 104, 3,   38,  19,  0,   104, 105, 5,   5,   0,   0,   105,
      106, 3,   12,  6,   0,   106, 11,  1,   0,   0,   0,   107, 108, 5,   10,  0,   0,   108, 113,
      3,   14,  7,   0,   109, 110, 5,   56,  0,   0,   110, 112, 3,   14,  7,   0,   111, 109, 1,
      0,   0,   0,   112, 115, 1,   0,   0,   0,   113, 111, 1,   0,   0,   0,   113, 114, 1,   0,
      0,   0,   114, 116, 1,   0,   0,   0,   115, 113, 1,   0,   0,   0,   116, 117, 5,   11,  0,
      0,   117, 119, 3,   16,  8,   0,   118, 120, 3,   18,  9,   0,   119, 118, 1,   0,   0,   0,
      119, 120, 1,   0,   0,   0,   120, 123, 1,   0,   0,   0,   121, 122, 5,   15,  0,   0,   122,
      124, 3,   20,  10,  0,   123, 121, 1,   0,   0,   0,   123, 124, 1,   0,   0,   0,   124, 135,
      1,   0,   0,   0,   125, 126, 5,   16,  0,   0,   126, 127, 5,   9,   0,   0,   127, 132, 3,
      20,  10,  0,   128, 129, 5,   56,  0,   0,   129, 131, 3,   20,  10,  0,   130, 128, 1,   0,
      0,   0,   131, 134, 1,   0,   0,   0,   132, 130, 1,   0,   0,   0,   132, 133, 1,   0,   0,
      0,   133, 136, 1,   0,   0,   0,   134, 132, 1,   0,   0,   0,   135, 125, 1,   0,   0,   0,
      135, 136, 1,   0,   0,   0,   136, 13,  1,   0,   0,   0,   137, 140, 3,   20,  10,  0,   138,
      139, 5,   5,   0,   0,   139, 141, 3,   38,  19,  0,   140, 138, 1,   0,   0,   0,   140, 141,
      1,   0,   0,   0,   141, 15,  1,   0,   0,   0,   142, 147, 3,   38,  19,  0,   143, 145, 5,
      5,   0,   0,   144, 143, 1,   0,   0,   0,   144, 145, 1,   0,   0,   0,   145, 146, 1,   0,
      0,   0,   146, 148, 3,   38,  19,  0,   147, 144, 1,   0,   0,   0,   147, 148, 1,   0,   0,
      0,   148, 17,  1,   0,   0,   0,   149, 151, 5,   12,  0,   0,   150, 149, 1,   0,   0,   0,
      150, 151, 1,   0,   0,   0,   151, 152, 1,   0,   0,   0,   152, 153, 5,   13,  0,   0,   153,
      154, 3,   16,  8,   0,   154, 155, 5,   14,  0,   0,   155, 156, 3,   20,  10,  0,   156, 19,
      1,   0,   0,   0,   157, 158, 3,   22,  11,  0,   158, 21,  1,   0,   0,   0,   159, 164, 3,
      24,  12,  0,   160, 161, 5,   29,  0,   0,   161, 163, 3,   24,  12,  0,   162, 160, 1,   0,
      0,   0,   163, 166, 1,   0,   0,   0,   164, 162, 1,   0,   0,   0,   164, 165, 1,   0,   0,
      0,   165, 23,  1,   0,   0,   0,   166, 164, 1,   0,   0,   0,   167, 172, 3,   26,  13,  0,
      168, 169, 5,   28,  0,   0,   169, 171, 3,   26,  13,  0,   170, 168, 1,   0,   0,   0,   171,
      174, 1,   0,   0,   0,   172, 170, 1,   0,   0,   0,   172, 173, 1,   0,   0,   0,   173, 25,
      1,   0,   0,   0,   174, 172, 1,   0,   0,   0,   175, 177, 5,   18,  0,   0,   176, 175, 1,
      0,   0,   0,   177, 180, 1,   0,   0,   0,   178, 176, 1,   0,   0,   0,   178, 179, 1,   0,
      0,   0,   179, 181, 1,   0,   0,   0,   180, 178, 1,   0,   0,   0,   181, 182, 3,   28,  14,
      0,   182, 27,  1,   0,   0,   0,   183, 191, 3,   30,  15,  0,   184, 185, 7,   1,   0,   0,
      185, 192, 3,   30,  15,  0,   186, 188, 5,   30,  0,   0,   187, 189, 5,   18,  0,   0,   188,
      187, 1,   0,   0,   0,   188, 189, 1,   0,   0,   0,   189, 190, 1,   0,   0,   0,   190, 192,
      5,   17,  0,   0,   191, 184, 1,   0,   0,   0,   191, 186, 1,   0,   0,   0,   191, 192, 1,
      0,   0,   0,   192, 29,  1,   0,   0,   0,   193, 198, 3,   32,  16,  0,   194, 195, 7,   2,
      0,   0,   195, 197, 3,   32,  16,  0,   196, 194, 1,   0,   0,   0,   197, 200, 1,   0,   0,
      0,   198, 196, 1,   0,   0,   0,   198, 199, 1,   0,   0,   0,   199, 31,  1,   0,   0,   0,
      200, 198, 1,   0,   0,   0,   201, 206, 3,   34,  17,  0,   202, 203, 7,   3,   0,   0,   203,
      205, 3,   34,  17,  0,   204, 202, 1,   0,   0,   0,   205, 208, 1,   0,   0,   0,   206, 204,
      1,   0,   0,   0,   206, 207, 1,   0,   0,   0,   207, 33,  1,   0,   0,   0,   208, 206, 1,
      0,   0,   0,   209, 211, 7,   2,   0,   0,   210, 209, 1,   0,   0,   0,   211, 214, 1,   0,
      0,   0,   212, 210, 1,   0,   0,   0,   212, 213, 1,   0,   0,   0,   213, 215, 1,   0,   0,
      0,   214, 212, 1,   0,   0,   0,   215, 216, 3,   36,  18,  0,   216, 35,  1,   0,   0,   0,
      217, 218, 5,   54,  0,   0,   218, 219, 3,   20,  10,  0,   219, 220, 5,   55,  0,   0,   220,
      257, 1,   0,   0,   0,   221, 222, 5,   27,  0,   0,   222, 223, 5,   54,  0,   0,   223, 224,
      3,   20,  10,  0,   224, 225, 5,   5,   0,   0,   225, 226, 3,   6,   3,   0,   226, 227, 5,
      55,  0,   0,   227, 257, 1,   0,   0,   0,   228, 229, 3,   38,  19,  0,   229, 238, 5,   54,
      0,   0,   230, 235, 3,   20,  10,  0,   231, 232, 5,   56,  0,   0,   232, 234, 3,   20,  10,
      0,   233, 231, 1,   0,   0,   0,   234, 237, 1,   0,   0,   0,   235, 233, 1,   0,   0,   0,
      235, 236, 1,   0,   0,   0,   236, 239, 1,   0,   0,   0,   237, 235, 1,   0,   0,   0,   238,
      230, 1,   0,   0,   0,   238, 239, 1,   0,   0,   0,   239, 240, 1,   0,   0,   0,   240, 241,
      5,   55,  0,   0,   241, 257, 1,   0,   0,   0,   242, 248, 3,   38,  19,  0,   243, 246, 5,
      58,  0,   0,   244, 247, 3,   38,  19,  0,   245, 247, 5,   61,  0,   0,   246, 244, 1,   0,
      0,   0,   246, 245, 1,   0,   0,   0,   247, 249, 1,   0,   0,   0,   248, 243, 1,   0,   0,
      0,   248, 249, 1,   0,   0,   0,   249, 257, 1,   0,   0,   0,   250, 257, 5,   69,  0,   0,
      251, 257, 5,   70,  0,   0,   252, 257, 5,   25,  0,   0,   253, 257, 5,   26,  0,   0,   254,
      257, 5,   17,  0,   0,   255, 257, 5,   61,  0,   0,   256, 217, 1,   0,   0,   0,   256, 221,
      1,   0,   0,   0,   256, 228, 1,   0,   0,   0,   256, 242, 1,   0,   0,   0,   256, 250, 1,
      0,   0,   0,   256, 251, 1,   0,   0,   0,   256, 252, 1,   0,   0,   0,   256, 253, 1,   0,
      0,   0,   256, 254, 1,   0,   0,   0,   256, 255, 1,   0,   0,   0,   257, 37,  1,   0,   0,
      0,   258, 259, 7,   4,   0,   0,   259, 39,  1,   0,   0,   0,   29,  43,  46,  59,  71,  81,
      88,  97,  113, 119, 123, 132, 135, 140, 144, 147, 150, 164, 172, 178, 188, 191, 198, 206, 212,
      235, 238, 246, 248, 256};
  staticData->serializedATN = antlr4::atn::SerializedATNView(
      serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  eventflowsqlParserStaticData = staticData.release();
}

} // namespace

EventFlowSqlParser::EventFlowSqlParser(TokenStream* input)
    : EventFlowSqlParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

EventFlowSqlParser::EventFlowSqlParser(TokenStream* input,
                                       const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  EventFlowSqlParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *eventflowsqlParserStaticData->atn, eventflowsqlParserStaticData->decisionToDFA,
      eventflowsqlParserStaticData->sharedContextCache, options);
}

EventFlowSqlParser::~EventFlowSqlParser() {
  delete _interpreter;
}

const atn::ATN& EventFlowSqlParser::getATN() const {
  return *eventflowsqlParserStaticData->atn;
}

std::string EventFlowSqlParser::getGrammarFileName() const {
  return "EventFlowSql.g4";
}

const std::vector<std::string>& EventFlowSqlParser::getRuleNames() const {
  return eventflowsqlParserStaticData->ruleNames;
}

const dfa::Vocabulary& EventFlowSqlParser::getVocabulary() const {
  return eventflowsqlParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView EventFlowSqlParser::getSerializedATN() const {
  return eventflowsqlParserStaticData->serializedATN;
}

//----------------- StatementContext
//------------------------------------------------------------------

EventFlowSqlParser::StatementContext::StatementContext(ParserRuleContext* parent,
                                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::StatementContext::EOF() {
  return getToken(EventFlowSqlParser::EOF, 0);
}

EventFlowSqlParser::CreateTableContext* EventFlowSqlParser::StatementContext::createTable() {
  return getRuleContext<EventFlowSqlParser::CreateTableContext>(0);
}

EventFlowSqlParser::CreateViewContext* EventFlowSqlParser::StatementContext::createView() {
  return getRuleContext<EventFlowSqlParser::CreateViewContext>(0);
}

EventFlowSqlParser::SelectQueryContext* EventFlowSqlParser::StatementContext::selectQuery() {
  return getRuleContext<EventFlowSqlParser::SelectQueryContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::StatementContext::SEMICOLON() {
  return getToken(EventFlowSqlParser::SEMICOLON, 0);
}

size_t EventFlowSqlParser::StatementContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleStatement;
}

EventFlowSqlParser::StatementContext* EventFlowSqlParser::statement() {
  StatementContext* _localctx = _tracker.createInstance<StatementContext>(_ctx, getState());
  enterRule(_localctx, 0, EventFlowSqlParser::RuleStatement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(43);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx)) {
    case 1: {
      setState(40);
      createTable();
      break;
    }

    case 2: {
      setState(41);
      createView();
      break;
    }

    case 3: {
      setState(42);
      selectQuery();
      break;
    }

    default:
      break;
    }
    setState(46);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::SEMICOLON) {
      setState(45);
      match(EventFlowSqlParser::SEMICOLON);
    }
    setState(48);
    match(EventFlowSqlParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CreateTableContext
//------------------------------------------------------------------

EventFlowSqlParser::CreateTableContext::CreateTableContext(ParserRuleContext* parent,
                                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_CREATE() {
  return getToken(EventFlowSqlParser::K_CREATE, 0);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_TABLE() {
  return getToken(EventFlowSqlParser::K_TABLE, 0);
}

EventFlowSqlParser::IdentifierContext* EventFlowSqlParser::CreateTableContext::identifier() {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::CreateTableContext::LPAREN() {
  return getTokens(EventFlowSqlParser::LPAREN);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::LPAREN(size_t i) {
  return getToken(EventFlowSqlParser::LPAREN, i);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_PRIMARY() {
  return getToken(EventFlowSqlParser::K_PRIMARY, 0);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_KEY() {
  return getToken(EventFlowSqlParser::K_KEY, 0);
}

std::vector<EventFlowSqlParser::IdentifierListContext*>
EventFlowSqlParser::CreateTableContext::identifierList() {
  return getRuleContexts<EventFlowSqlParser::IdentifierListContext>();
}

EventFlowSqlParser::IdentifierListContext*
EventFlowSqlParser::CreateTableContext::identifierList(size_t i) {
  return getRuleContext<EventFlowSqlParser::IdentifierListContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::CreateTableContext::RPAREN() {
  return getTokens(EventFlowSqlParser::RPAREN);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::RPAREN(size_t i) {
  return getToken(EventFlowSqlParser::RPAREN, i);
}

std::vector<EventFlowSqlParser::ColumnDefinitionContext*>
EventFlowSqlParser::CreateTableContext::columnDefinition() {
  return getRuleContexts<EventFlowSqlParser::ColumnDefinitionContext>();
}

EventFlowSqlParser::ColumnDefinitionContext*
EventFlowSqlParser::CreateTableContext::columnDefinition(size_t i) {
  return getRuleContext<EventFlowSqlParser::ColumnDefinitionContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::CreateTableContext::COMMA() {
  return getTokens(EventFlowSqlParser::COMMA);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::COMMA(size_t i) {
  return getToken(EventFlowSqlParser::COMMA, i);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_PARTITION() {
  return getToken(EventFlowSqlParser::K_PARTITION, 0);
}

tree::TerminalNode* EventFlowSqlParser::CreateTableContext::K_BY() {
  return getToken(EventFlowSqlParser::K_BY, 0);
}

size_t EventFlowSqlParser::CreateTableContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleCreateTable;
}

EventFlowSqlParser::CreateTableContext* EventFlowSqlParser::createTable() {
  CreateTableContext* _localctx = _tracker.createInstance<CreateTableContext>(_ctx, getState());
  enterRule(_localctx, 2, EventFlowSqlParser::RuleCreateTable);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(50);
    match(EventFlowSqlParser::K_CREATE);
    setState(51);
    match(EventFlowSqlParser::K_TABLE);
    setState(52);
    identifier();
    setState(53);
    match(EventFlowSqlParser::LPAREN);
    setState(59);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::QUOTED_IDENTIFIER

           || _la == EventFlowSqlParser::IDENTIFIER) {
      setState(54);
      columnDefinition();
      setState(55);
      match(EventFlowSqlParser::COMMA);
      setState(61);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(62);
    match(EventFlowSqlParser::K_PRIMARY);
    setState(63);
    match(EventFlowSqlParser::K_KEY);
    setState(64);
    match(EventFlowSqlParser::LPAREN);
    setState(65);
    identifierList();
    setState(66);
    match(EventFlowSqlParser::RPAREN);
    setState(71);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::COMMA) {
      setState(67);
      match(EventFlowSqlParser::COMMA);
      setState(68);
      columnDefinition();
      setState(73);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(74);
    match(EventFlowSqlParser::RPAREN);
    setState(81);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_PARTITION) {
      setState(75);
      match(EventFlowSqlParser::K_PARTITION);
      setState(76);
      match(EventFlowSqlParser::K_BY);
      setState(77);
      match(EventFlowSqlParser::LPAREN);
      setState(78);
      identifierList();
      setState(79);
      match(EventFlowSqlParser::RPAREN);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnDefinitionContext
//------------------------------------------------------------------

EventFlowSqlParser::ColumnDefinitionContext::ColumnDefinitionContext(ParserRuleContext* parent,
                                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

EventFlowSqlParser::IdentifierContext* EventFlowSqlParser::ColumnDefinitionContext::identifier() {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(0);
}

EventFlowSqlParser::DataTypeContext* EventFlowSqlParser::ColumnDefinitionContext::dataType() {
  return getRuleContext<EventFlowSqlParser::DataTypeContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::ColumnDefinitionContext::K_NULL() {
  return getToken(EventFlowSqlParser::K_NULL, 0);
}

tree::TerminalNode* EventFlowSqlParser::ColumnDefinitionContext::K_NOT() {
  return getToken(EventFlowSqlParser::K_NOT, 0);
}

size_t EventFlowSqlParser::ColumnDefinitionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleColumnDefinition;
}

EventFlowSqlParser::ColumnDefinitionContext* EventFlowSqlParser::columnDefinition() {
  ColumnDefinitionContext* _localctx =
      _tracker.createInstance<ColumnDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 4, EventFlowSqlParser::RuleColumnDefinition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(83);
    identifier();
    setState(84);
    dataType();
    setState(88);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
    case EventFlowSqlParser::K_NULL: {
      setState(85);
      match(EventFlowSqlParser::K_NULL);
      break;
    }

    case EventFlowSqlParser::K_NOT: {
      setState(86);
      match(EventFlowSqlParser::K_NOT);
      setState(87);
      match(EventFlowSqlParser::K_NULL);
      break;
    }

    case EventFlowSqlParser::RPAREN:
    case EventFlowSqlParser::COMMA: {
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataTypeContext
//------------------------------------------------------------------

EventFlowSqlParser::DataTypeContext::DataTypeContext(ParserRuleContext* parent,
                                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_BOOL() {
  return getToken(EventFlowSqlParser::K_BOOL, 0);
}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_INT64() {
  return getToken(EventFlowSqlParser::K_INT64, 0);
}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_FLOAT64() {
  return getToken(EventFlowSqlParser::K_FLOAT64, 0);
}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_STRING() {
  return getToken(EventFlowSqlParser::K_STRING, 0);
}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_TIMESTAMP_MICROS() {
  return getToken(EventFlowSqlParser::K_TIMESTAMP_MICROS, 0);
}

tree::TerminalNode* EventFlowSqlParser::DataTypeContext::K_FLOAT_VECTOR() {
  return getToken(EventFlowSqlParser::K_FLOAT_VECTOR, 0);
}

size_t EventFlowSqlParser::DataTypeContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleDataType;
}

EventFlowSqlParser::DataTypeContext* EventFlowSqlParser::dataType() {
  DataTypeContext* _localctx = _tracker.createInstance<DataTypeContext>(_ctx, getState());
  enterRule(_localctx, 6, EventFlowSqlParser::RuleDataType);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(90);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) &
            ((1ULL << EventFlowSqlParser::K_BOOL) | (1ULL << EventFlowSqlParser::K_INT64) |
             (1ULL << EventFlowSqlParser::K_FLOAT64) | (1ULL << EventFlowSqlParser::K_STRING) |
             (1ULL << EventFlowSqlParser::K_TIMESTAMP_MICROS) |
             (1ULL << EventFlowSqlParser::K_FLOAT_VECTOR))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierListContext
//------------------------------------------------------------------

EventFlowSqlParser::IdentifierListContext::IdentifierListContext(ParserRuleContext* parent,
                                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::IdentifierContext*>
EventFlowSqlParser::IdentifierListContext::identifier() {
  return getRuleContexts<EventFlowSqlParser::IdentifierContext>();
}

EventFlowSqlParser::IdentifierContext*
EventFlowSqlParser::IdentifierListContext::identifier(size_t i) {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::IdentifierListContext::COMMA() {
  return getTokens(EventFlowSqlParser::COMMA);
}

tree::TerminalNode* EventFlowSqlParser::IdentifierListContext::COMMA(size_t i) {
  return getToken(EventFlowSqlParser::COMMA, i);
}

size_t EventFlowSqlParser::IdentifierListContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleIdentifierList;
}

EventFlowSqlParser::IdentifierListContext* EventFlowSqlParser::identifierList() {
  IdentifierListContext* _localctx =
      _tracker.createInstance<IdentifierListContext>(_ctx, getState());
  enterRule(_localctx, 8, EventFlowSqlParser::RuleIdentifierList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(92);
    identifier();
    setState(97);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::COMMA) {
      setState(93);
      match(EventFlowSqlParser::COMMA);
      setState(94);
      identifier();
      setState(99);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CreateViewContext
//------------------------------------------------------------------

EventFlowSqlParser::CreateViewContext::CreateViewContext(ParserRuleContext* parent,
                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::CreateViewContext::K_CREATE() {
  return getToken(EventFlowSqlParser::K_CREATE, 0);
}

tree::TerminalNode* EventFlowSqlParser::CreateViewContext::K_MATERIALIZED() {
  return getToken(EventFlowSqlParser::K_MATERIALIZED, 0);
}

tree::TerminalNode* EventFlowSqlParser::CreateViewContext::K_VIEW() {
  return getToken(EventFlowSqlParser::K_VIEW, 0);
}

EventFlowSqlParser::IdentifierContext* EventFlowSqlParser::CreateViewContext::identifier() {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::CreateViewContext::K_AS() {
  return getToken(EventFlowSqlParser::K_AS, 0);
}

EventFlowSqlParser::SelectQueryContext* EventFlowSqlParser::CreateViewContext::selectQuery() {
  return getRuleContext<EventFlowSqlParser::SelectQueryContext>(0);
}

size_t EventFlowSqlParser::CreateViewContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleCreateView;
}

EventFlowSqlParser::CreateViewContext* EventFlowSqlParser::createView() {
  CreateViewContext* _localctx = _tracker.createInstance<CreateViewContext>(_ctx, getState());
  enterRule(_localctx, 10, EventFlowSqlParser::RuleCreateView);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(100);
    match(EventFlowSqlParser::K_CREATE);
    setState(101);
    match(EventFlowSqlParser::K_MATERIALIZED);
    setState(102);
    match(EventFlowSqlParser::K_VIEW);
    setState(103);
    identifier();
    setState(104);
    match(EventFlowSqlParser::K_AS);
    setState(105);
    selectQuery();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectQueryContext
//------------------------------------------------------------------

EventFlowSqlParser::SelectQueryContext::SelectQueryContext(ParserRuleContext* parent,
                                                           size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::K_SELECT() {
  return getToken(EventFlowSqlParser::K_SELECT, 0);
}

std::vector<EventFlowSqlParser::SelectItemContext*>
EventFlowSqlParser::SelectQueryContext::selectItem() {
  return getRuleContexts<EventFlowSqlParser::SelectItemContext>();
}

EventFlowSqlParser::SelectItemContext*
EventFlowSqlParser::SelectQueryContext::selectItem(size_t i) {
  return getRuleContext<EventFlowSqlParser::SelectItemContext>(i);
}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::K_FROM() {
  return getToken(EventFlowSqlParser::K_FROM, 0);
}

EventFlowSqlParser::TableReferenceContext*
EventFlowSqlParser::SelectQueryContext::tableReference() {
  return getRuleContext<EventFlowSqlParser::TableReferenceContext>(0);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::SelectQueryContext::COMMA() {
  return getTokens(EventFlowSqlParser::COMMA);
}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::COMMA(size_t i) {
  return getToken(EventFlowSqlParser::COMMA, i);
}

EventFlowSqlParser::JoinClauseContext* EventFlowSqlParser::SelectQueryContext::joinClause() {
  return getRuleContext<EventFlowSqlParser::JoinClauseContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::K_WHERE() {
  return getToken(EventFlowSqlParser::K_WHERE, 0);
}

std::vector<EventFlowSqlParser::ExpressionContext*>
EventFlowSqlParser::SelectQueryContext::expression() {
  return getRuleContexts<EventFlowSqlParser::ExpressionContext>();
}

EventFlowSqlParser::ExpressionContext*
EventFlowSqlParser::SelectQueryContext::expression(size_t i) {
  return getRuleContext<EventFlowSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::K_GROUP() {
  return getToken(EventFlowSqlParser::K_GROUP, 0);
}

tree::TerminalNode* EventFlowSqlParser::SelectQueryContext::K_BY() {
  return getToken(EventFlowSqlParser::K_BY, 0);
}

size_t EventFlowSqlParser::SelectQueryContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleSelectQuery;
}

EventFlowSqlParser::SelectQueryContext* EventFlowSqlParser::selectQuery() {
  SelectQueryContext* _localctx = _tracker.createInstance<SelectQueryContext>(_ctx, getState());
  enterRule(_localctx, 12, EventFlowSqlParser::RuleSelectQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(107);
    match(EventFlowSqlParser::K_SELECT);
    setState(108);
    selectItem();
    setState(113);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::COMMA) {
      setState(109);
      match(EventFlowSqlParser::COMMA);
      setState(110);
      selectItem();
      setState(115);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(116);
    match(EventFlowSqlParser::K_FROM);
    setState(117);
    tableReference();
    setState(119);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_INNER

        || _la == EventFlowSqlParser::K_JOIN) {
      setState(118);
      joinClause();
    }
    setState(123);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_WHERE) {
      setState(121);
      match(EventFlowSqlParser::K_WHERE);
      setState(122);
      expression();
    }
    setState(135);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_GROUP) {
      setState(125);
      match(EventFlowSqlParser::K_GROUP);
      setState(126);
      match(EventFlowSqlParser::K_BY);
      setState(127);
      expression();
      setState(132);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == EventFlowSqlParser::COMMA) {
        setState(128);
        match(EventFlowSqlParser::COMMA);
        setState(129);
        expression();
        setState(134);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectItemContext
//------------------------------------------------------------------

EventFlowSqlParser::SelectItemContext::SelectItemContext(ParserRuleContext* parent,
                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

EventFlowSqlParser::ExpressionContext* EventFlowSqlParser::SelectItemContext::expression() {
  return getRuleContext<EventFlowSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::SelectItemContext::K_AS() {
  return getToken(EventFlowSqlParser::K_AS, 0);
}

EventFlowSqlParser::IdentifierContext* EventFlowSqlParser::SelectItemContext::identifier() {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(0);
}

size_t EventFlowSqlParser::SelectItemContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleSelectItem;
}

EventFlowSqlParser::SelectItemContext* EventFlowSqlParser::selectItem() {
  SelectItemContext* _localctx = _tracker.createInstance<SelectItemContext>(_ctx, getState());
  enterRule(_localctx, 14, EventFlowSqlParser::RuleSelectItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(137);
    expression();
    setState(140);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_AS) {
      setState(138);
      match(EventFlowSqlParser::K_AS);
      setState(139);
      identifier();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableReferenceContext
//------------------------------------------------------------------

EventFlowSqlParser::TableReferenceContext::TableReferenceContext(ParserRuleContext* parent,
                                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::IdentifierContext*>
EventFlowSqlParser::TableReferenceContext::identifier() {
  return getRuleContexts<EventFlowSqlParser::IdentifierContext>();
}

EventFlowSqlParser::IdentifierContext*
EventFlowSqlParser::TableReferenceContext::identifier(size_t i) {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(i);
}

tree::TerminalNode* EventFlowSqlParser::TableReferenceContext::K_AS() {
  return getToken(EventFlowSqlParser::K_AS, 0);
}

size_t EventFlowSqlParser::TableReferenceContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleTableReference;
}

EventFlowSqlParser::TableReferenceContext* EventFlowSqlParser::tableReference() {
  TableReferenceContext* _localctx =
      _tracker.createInstance<TableReferenceContext>(_ctx, getState());
  enterRule(_localctx, 16, EventFlowSqlParser::RuleTableReference);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(142);
    identifier();
    setState(147);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_AS || _la == EventFlowSqlParser::QUOTED_IDENTIFIER

        || _la == EventFlowSqlParser::IDENTIFIER) {
      setState(144);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == EventFlowSqlParser::K_AS) {
        setState(143);
        match(EventFlowSqlParser::K_AS);
      }
      setState(146);
      identifier();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinClauseContext
//------------------------------------------------------------------

EventFlowSqlParser::JoinClauseContext::JoinClauseContext(ParserRuleContext* parent,
                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::JoinClauseContext::K_JOIN() {
  return getToken(EventFlowSqlParser::K_JOIN, 0);
}

EventFlowSqlParser::TableReferenceContext* EventFlowSqlParser::JoinClauseContext::tableReference() {
  return getRuleContext<EventFlowSqlParser::TableReferenceContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::JoinClauseContext::K_ON() {
  return getToken(EventFlowSqlParser::K_ON, 0);
}

EventFlowSqlParser::ExpressionContext* EventFlowSqlParser::JoinClauseContext::expression() {
  return getRuleContext<EventFlowSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* EventFlowSqlParser::JoinClauseContext::K_INNER() {
  return getToken(EventFlowSqlParser::K_INNER, 0);
}

size_t EventFlowSqlParser::JoinClauseContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleJoinClause;
}

EventFlowSqlParser::JoinClauseContext* EventFlowSqlParser::joinClause() {
  JoinClauseContext* _localctx = _tracker.createInstance<JoinClauseContext>(_ctx, getState());
  enterRule(_localctx, 18, EventFlowSqlParser::RuleJoinClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(150);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == EventFlowSqlParser::K_INNER) {
      setState(149);
      match(EventFlowSqlParser::K_INNER);
    }
    setState(152);
    match(EventFlowSqlParser::K_JOIN);
    setState(153);
    tableReference();
    setState(154);
    match(EventFlowSqlParser::K_ON);
    setState(155);
    expression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::ExpressionContext::ExpressionContext(ParserRuleContext* parent,
                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

EventFlowSqlParser::OrExpressionContext* EventFlowSqlParser::ExpressionContext::orExpression() {
  return getRuleContext<EventFlowSqlParser::OrExpressionContext>(0);
}

size_t EventFlowSqlParser::ExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleExpression;
}

EventFlowSqlParser::ExpressionContext* EventFlowSqlParser::expression() {
  ExpressionContext* _localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 20, EventFlowSqlParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(157);
    orExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::OrExpressionContext::OrExpressionContext(ParserRuleContext* parent,
                                                             size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::AndExpressionContext*>
EventFlowSqlParser::OrExpressionContext::andExpression() {
  return getRuleContexts<EventFlowSqlParser::AndExpressionContext>();
}

EventFlowSqlParser::AndExpressionContext*
EventFlowSqlParser::OrExpressionContext::andExpression(size_t i) {
  return getRuleContext<EventFlowSqlParser::AndExpressionContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::OrExpressionContext::K_OR() {
  return getTokens(EventFlowSqlParser::K_OR);
}

tree::TerminalNode* EventFlowSqlParser::OrExpressionContext::K_OR(size_t i) {
  return getToken(EventFlowSqlParser::K_OR, i);
}

size_t EventFlowSqlParser::OrExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleOrExpression;
}

EventFlowSqlParser::OrExpressionContext* EventFlowSqlParser::orExpression() {
  OrExpressionContext* _localctx = _tracker.createInstance<OrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 22, EventFlowSqlParser::RuleOrExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(159);
    andExpression();
    setState(164);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::K_OR) {
      setState(160);
      match(EventFlowSqlParser::K_OR);
      setState(161);
      andExpression();
      setState(166);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AndExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::AndExpressionContext::AndExpressionContext(ParserRuleContext* parent,
                                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::NotExpressionContext*>
EventFlowSqlParser::AndExpressionContext::notExpression() {
  return getRuleContexts<EventFlowSqlParser::NotExpressionContext>();
}

EventFlowSqlParser::NotExpressionContext*
EventFlowSqlParser::AndExpressionContext::notExpression(size_t i) {
  return getRuleContext<EventFlowSqlParser::NotExpressionContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::AndExpressionContext::K_AND() {
  return getTokens(EventFlowSqlParser::K_AND);
}

tree::TerminalNode* EventFlowSqlParser::AndExpressionContext::K_AND(size_t i) {
  return getToken(EventFlowSqlParser::K_AND, i);
}

size_t EventFlowSqlParser::AndExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleAndExpression;
}

EventFlowSqlParser::AndExpressionContext* EventFlowSqlParser::andExpression() {
  AndExpressionContext* _localctx = _tracker.createInstance<AndExpressionContext>(_ctx, getState());
  enterRule(_localctx, 24, EventFlowSqlParser::RuleAndExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(167);
    notExpression();
    setState(172);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::K_AND) {
      setState(168);
      match(EventFlowSqlParser::K_AND);
      setState(169);
      notExpression();
      setState(174);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NotExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::NotExpressionContext::NotExpressionContext(ParserRuleContext* parent,
                                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

EventFlowSqlParser::ComparisonExpressionContext*
EventFlowSqlParser::NotExpressionContext::comparisonExpression() {
  return getRuleContext<EventFlowSqlParser::ComparisonExpressionContext>(0);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::NotExpressionContext::K_NOT() {
  return getTokens(EventFlowSqlParser::K_NOT);
}

tree::TerminalNode* EventFlowSqlParser::NotExpressionContext::K_NOT(size_t i) {
  return getToken(EventFlowSqlParser::K_NOT, i);
}

size_t EventFlowSqlParser::NotExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleNotExpression;
}

EventFlowSqlParser::NotExpressionContext* EventFlowSqlParser::notExpression() {
  NotExpressionContext* _localctx = _tracker.createInstance<NotExpressionContext>(_ctx, getState());
  enterRule(_localctx, 26, EventFlowSqlParser::RuleNotExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(178);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::K_NOT) {
      setState(175);
      match(EventFlowSqlParser::K_NOT);
      setState(180);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(181);
    comparisonExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ComparisonExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::ComparisonExpressionContext::ComparisonExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::AdditiveExpressionContext*>
EventFlowSqlParser::ComparisonExpressionContext::additiveExpression() {
  return getRuleContexts<EventFlowSqlParser::AdditiveExpressionContext>();
}

EventFlowSqlParser::AdditiveExpressionContext*
EventFlowSqlParser::ComparisonExpressionContext::additiveExpression(size_t i) {
  return getRuleContext<EventFlowSqlParser::AdditiveExpressionContext>(i);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::K_IS() {
  return getToken(EventFlowSqlParser::K_IS, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::K_NULL() {
  return getToken(EventFlowSqlParser::K_NULL, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::EQ() {
  return getToken(EventFlowSqlParser::EQ, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::NEQ() {
  return getToken(EventFlowSqlParser::NEQ, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::LT() {
  return getToken(EventFlowSqlParser::LT, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::LE() {
  return getToken(EventFlowSqlParser::LE, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::GT() {
  return getToken(EventFlowSqlParser::GT, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::GE() {
  return getToken(EventFlowSqlParser::GE, 0);
}

tree::TerminalNode* EventFlowSqlParser::ComparisonExpressionContext::K_NOT() {
  return getToken(EventFlowSqlParser::K_NOT, 0);
}

size_t EventFlowSqlParser::ComparisonExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleComparisonExpression;
}

EventFlowSqlParser::ComparisonExpressionContext* EventFlowSqlParser::comparisonExpression() {
  ComparisonExpressionContext* _localctx =
      _tracker.createInstance<ComparisonExpressionContext>(_ctx, getState());
  enterRule(_localctx, 28, EventFlowSqlParser::RuleComparisonExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(183);
    additiveExpression();
    setState(191);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
    case EventFlowSqlParser::EQ:
    case EventFlowSqlParser::NEQ:
    case EventFlowSqlParser::LE:
    case EventFlowSqlParser::LT:
    case EventFlowSqlParser::GE:
    case EventFlowSqlParser::GT: {
      setState(184);
      _la = _input->LA(1);
      if (!(((((_la - 63) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 63)) &
              ((1ULL << (EventFlowSqlParser::EQ - 63)) | (1ULL << (EventFlowSqlParser::NEQ - 63)) |
               (1ULL << (EventFlowSqlParser::LE - 63)) | (1ULL << (EventFlowSqlParser::LT - 63)) |
               (1ULL << (EventFlowSqlParser::GE - 63)) |
               (1ULL << (EventFlowSqlParser::GT - 63)))) != 0))) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(185);
      additiveExpression();
      break;
    }

    case EventFlowSqlParser::K_IS: {
      setState(186);
      match(EventFlowSqlParser::K_IS);
      setState(188);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == EventFlowSqlParser::K_NOT) {
        setState(187);
        match(EventFlowSqlParser::K_NOT);
      }
      setState(190);
      match(EventFlowSqlParser::K_NULL);
      break;
    }

    case EventFlowSqlParser::EOF:
    case EventFlowSqlParser::K_AS:
    case EventFlowSqlParser::K_FROM:
    case EventFlowSqlParser::K_WHERE:
    case EventFlowSqlParser::K_GROUP:
    case EventFlowSqlParser::K_AND:
    case EventFlowSqlParser::K_OR:
    case EventFlowSqlParser::RPAREN:
    case EventFlowSqlParser::COMMA:
    case EventFlowSqlParser::SEMICOLON: {
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AdditiveExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::AdditiveExpressionContext::AdditiveExpressionContext(ParserRuleContext* parent,
                                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::MultiplicativeExpressionContext*>
EventFlowSqlParser::AdditiveExpressionContext::multiplicativeExpression() {
  return getRuleContexts<EventFlowSqlParser::MultiplicativeExpressionContext>();
}

EventFlowSqlParser::MultiplicativeExpressionContext*
EventFlowSqlParser::AdditiveExpressionContext::multiplicativeExpression(size_t i) {
  return getRuleContext<EventFlowSqlParser::MultiplicativeExpressionContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::AdditiveExpressionContext::PLUS() {
  return getTokens(EventFlowSqlParser::PLUS);
}

tree::TerminalNode* EventFlowSqlParser::AdditiveExpressionContext::PLUS(size_t i) {
  return getToken(EventFlowSqlParser::PLUS, i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::AdditiveExpressionContext::MINUS() {
  return getTokens(EventFlowSqlParser::MINUS);
}

tree::TerminalNode* EventFlowSqlParser::AdditiveExpressionContext::MINUS(size_t i) {
  return getToken(EventFlowSqlParser::MINUS, i);
}

size_t EventFlowSqlParser::AdditiveExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleAdditiveExpression;
}

EventFlowSqlParser::AdditiveExpressionContext* EventFlowSqlParser::additiveExpression() {
  AdditiveExpressionContext* _localctx =
      _tracker.createInstance<AdditiveExpressionContext>(_ctx, getState());
  enterRule(_localctx, 30, EventFlowSqlParser::RuleAdditiveExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(193);
    multiplicativeExpression();
    setState(198);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::PLUS

           || _la == EventFlowSqlParser::MINUS) {
      setState(194);
      _la = _input->LA(1);
      if (!(_la == EventFlowSqlParser::PLUS

            || _la == EventFlowSqlParser::MINUS)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(195);
      multiplicativeExpression();
      setState(200);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::MultiplicativeExpressionContext::MultiplicativeExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<EventFlowSqlParser::UnaryExpressionContext*>
EventFlowSqlParser::MultiplicativeExpressionContext::unaryExpression() {
  return getRuleContexts<EventFlowSqlParser::UnaryExpressionContext>();
}

EventFlowSqlParser::UnaryExpressionContext*
EventFlowSqlParser::MultiplicativeExpressionContext::unaryExpression(size_t i) {
  return getRuleContext<EventFlowSqlParser::UnaryExpressionContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::MultiplicativeExpressionContext::STAR() {
  return getTokens(EventFlowSqlParser::STAR);
}

tree::TerminalNode* EventFlowSqlParser::MultiplicativeExpressionContext::STAR(size_t i) {
  return getToken(EventFlowSqlParser::STAR, i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::MultiplicativeExpressionContext::SLASH() {
  return getTokens(EventFlowSqlParser::SLASH);
}

tree::TerminalNode* EventFlowSqlParser::MultiplicativeExpressionContext::SLASH(size_t i) {
  return getToken(EventFlowSqlParser::SLASH, i);
}

size_t EventFlowSqlParser::MultiplicativeExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleMultiplicativeExpression;
}

EventFlowSqlParser::MultiplicativeExpressionContext*
EventFlowSqlParser::multiplicativeExpression() {
  MultiplicativeExpressionContext* _localctx =
      _tracker.createInstance<MultiplicativeExpressionContext>(_ctx, getState());
  enterRule(_localctx, 32, EventFlowSqlParser::RuleMultiplicativeExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(201);
    unaryExpression();
    setState(206);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::STAR

           || _la == EventFlowSqlParser::SLASH) {
      setState(202);
      _la = _input->LA(1);
      if (!(_la == EventFlowSqlParser::STAR

            || _la == EventFlowSqlParser::SLASH)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(203);
      unaryExpression();
      setState(208);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnaryExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::UnaryExpressionContext::UnaryExpressionContext(ParserRuleContext* parent,
                                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

EventFlowSqlParser::PrimaryExpressionContext*
EventFlowSqlParser::UnaryExpressionContext::primaryExpression() {
  return getRuleContext<EventFlowSqlParser::PrimaryExpressionContext>(0);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::UnaryExpressionContext::PLUS() {
  return getTokens(EventFlowSqlParser::PLUS);
}

tree::TerminalNode* EventFlowSqlParser::UnaryExpressionContext::PLUS(size_t i) {
  return getToken(EventFlowSqlParser::PLUS, i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::UnaryExpressionContext::MINUS() {
  return getTokens(EventFlowSqlParser::MINUS);
}

tree::TerminalNode* EventFlowSqlParser::UnaryExpressionContext::MINUS(size_t i) {
  return getToken(EventFlowSqlParser::MINUS, i);
}

size_t EventFlowSqlParser::UnaryExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleUnaryExpression;
}

EventFlowSqlParser::UnaryExpressionContext* EventFlowSqlParser::unaryExpression() {
  UnaryExpressionContext* _localctx =
      _tracker.createInstance<UnaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 34, EventFlowSqlParser::RuleUnaryExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(212);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == EventFlowSqlParser::PLUS

           || _la == EventFlowSqlParser::MINUS) {
      setState(209);
      _la = _input->LA(1);
      if (!(_la == EventFlowSqlParser::PLUS

            || _la == EventFlowSqlParser::MINUS)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(214);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(215);
    primaryExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryExpressionContext
//------------------------------------------------------------------

EventFlowSqlParser::PrimaryExpressionContext::PrimaryExpressionContext(ParserRuleContext* parent,
                                                                       size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::LPAREN() {
  return getToken(EventFlowSqlParser::LPAREN, 0);
}

std::vector<EventFlowSqlParser::ExpressionContext*>
EventFlowSqlParser::PrimaryExpressionContext::expression() {
  return getRuleContexts<EventFlowSqlParser::ExpressionContext>();
}

EventFlowSqlParser::ExpressionContext*
EventFlowSqlParser::PrimaryExpressionContext::expression(size_t i) {
  return getRuleContext<EventFlowSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::RPAREN() {
  return getToken(EventFlowSqlParser::RPAREN, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::K_CAST() {
  return getToken(EventFlowSqlParser::K_CAST, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::K_AS() {
  return getToken(EventFlowSqlParser::K_AS, 0);
}

EventFlowSqlParser::DataTypeContext* EventFlowSqlParser::PrimaryExpressionContext::dataType() {
  return getRuleContext<EventFlowSqlParser::DataTypeContext>(0);
}

std::vector<EventFlowSqlParser::IdentifierContext*>
EventFlowSqlParser::PrimaryExpressionContext::identifier() {
  return getRuleContexts<EventFlowSqlParser::IdentifierContext>();
}

EventFlowSqlParser::IdentifierContext*
EventFlowSqlParser::PrimaryExpressionContext::identifier(size_t i) {
  return getRuleContext<EventFlowSqlParser::IdentifierContext>(i);
}

std::vector<tree::TerminalNode*> EventFlowSqlParser::PrimaryExpressionContext::COMMA() {
  return getTokens(EventFlowSqlParser::COMMA);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::COMMA(size_t i) {
  return getToken(EventFlowSqlParser::COMMA, i);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::DOT() {
  return getToken(EventFlowSqlParser::DOT, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::STAR() {
  return getToken(EventFlowSqlParser::STAR, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::NUMBER() {
  return getToken(EventFlowSqlParser::NUMBER, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::STRING_LITERAL() {
  return getToken(EventFlowSqlParser::STRING_LITERAL, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::K_TRUE() {
  return getToken(EventFlowSqlParser::K_TRUE, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::K_FALSE() {
  return getToken(EventFlowSqlParser::K_FALSE, 0);
}

tree::TerminalNode* EventFlowSqlParser::PrimaryExpressionContext::K_NULL() {
  return getToken(EventFlowSqlParser::K_NULL, 0);
}

size_t EventFlowSqlParser::PrimaryExpressionContext::getRuleIndex() const {
  return EventFlowSqlParser::RulePrimaryExpression;
}

EventFlowSqlParser::PrimaryExpressionContext* EventFlowSqlParser::primaryExpression() {
  PrimaryExpressionContext* _localctx =
      _tracker.createInstance<PrimaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 36, EventFlowSqlParser::RulePrimaryExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(256);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 28, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(217);
      match(EventFlowSqlParser::LPAREN);
      setState(218);
      expression();
      setState(219);
      match(EventFlowSqlParser::RPAREN);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(221);
      match(EventFlowSqlParser::K_CAST);
      setState(222);
      match(EventFlowSqlParser::LPAREN);
      setState(223);
      expression();
      setState(224);
      match(EventFlowSqlParser::K_AS);
      setState(225);
      dataType();
      setState(226);
      match(EventFlowSqlParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(228);
      identifier();
      setState(229);
      match(EventFlowSqlParser::LPAREN);
      setState(238);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (((((_la - 17) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 17)) & ((1ULL << (EventFlowSqlParser::K_NULL - 17)) |
                                    (1ULL << (EventFlowSqlParser::K_NOT - 17)) |
                                    (1ULL << (EventFlowSqlParser::K_TRUE - 17)) |
                                    (1ULL << (EventFlowSqlParser::K_FALSE - 17)) |
                                    (1ULL << (EventFlowSqlParser::K_CAST - 17)) |
                                    (1ULL << (EventFlowSqlParser::LPAREN - 17)) |
                                    (1ULL << (EventFlowSqlParser::PLUS - 17)) |
                                    (1ULL << (EventFlowSqlParser::MINUS - 17)) |
                                    (1ULL << (EventFlowSqlParser::STAR - 17)) |
                                    (1ULL << (EventFlowSqlParser::NUMBER - 17)) |
                                    (1ULL << (EventFlowSqlParser::STRING_LITERAL - 17)) |
                                    (1ULL << (EventFlowSqlParser::QUOTED_IDENTIFIER - 17)) |
                                    (1ULL << (EventFlowSqlParser::IDENTIFIER - 17)))) != 0)) {
        setState(230);
        expression();
        setState(235);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == EventFlowSqlParser::COMMA) {
          setState(231);
          match(EventFlowSqlParser::COMMA);
          setState(232);
          expression();
          setState(237);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
      }
      setState(240);
      match(EventFlowSqlParser::RPAREN);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(242);
      identifier();
      setState(248);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == EventFlowSqlParser::DOT) {
        setState(243);
        match(EventFlowSqlParser::DOT);
        setState(246);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
        case EventFlowSqlParser::QUOTED_IDENTIFIER:
        case EventFlowSqlParser::IDENTIFIER: {
          setState(244);
          identifier();
          break;
        }

        case EventFlowSqlParser::STAR: {
          setState(245);
          match(EventFlowSqlParser::STAR);
          break;
        }

        default:
          throw NoViableAltException(this);
        }
      }
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(250);
      match(EventFlowSqlParser::NUMBER);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(251);
      match(EventFlowSqlParser::STRING_LITERAL);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(252);
      match(EventFlowSqlParser::K_TRUE);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(253);
      match(EventFlowSqlParser::K_FALSE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(254);
      match(EventFlowSqlParser::K_NULL);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(255);
      match(EventFlowSqlParser::STAR);
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext
//------------------------------------------------------------------

EventFlowSqlParser::IdentifierContext::IdentifierContext(ParserRuleContext* parent,
                                                         size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* EventFlowSqlParser::IdentifierContext::IDENTIFIER() {
  return getToken(EventFlowSqlParser::IDENTIFIER, 0);
}

tree::TerminalNode* EventFlowSqlParser::IdentifierContext::QUOTED_IDENTIFIER() {
  return getToken(EventFlowSqlParser::QUOTED_IDENTIFIER, 0);
}

size_t EventFlowSqlParser::IdentifierContext::getRuleIndex() const {
  return EventFlowSqlParser::RuleIdentifier;
}

EventFlowSqlParser::IdentifierContext* EventFlowSqlParser::identifier() {
  IdentifierContext* _localctx = _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 38, EventFlowSqlParser::RuleIdentifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(258);
    _la = _input->LA(1);
    if (!(_la == EventFlowSqlParser::QUOTED_IDENTIFIER

          || _la == EventFlowSqlParser::IDENTIFIER)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

void EventFlowSqlParser::initialize() {
  std::call_once(eventflowsqlParserOnceFlag, eventflowsqlParserInitialize);
}
