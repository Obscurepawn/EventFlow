
// Generated from EventFlowSql.g4 by ANTLR 4.10

#pragma once

#include "antlr4-runtime.h"

class EventFlowSqlLexer : public antlr4::Lexer {
public:
  enum {
    K_CREATE = 1,
    K_TABLE = 2,
    K_MATERIALIZED = 3,
    K_VIEW = 4,
    K_AS = 5,
    K_PRIMARY = 6,
    K_KEY = 7,
    K_PARTITION = 8,
    K_BY = 9,
    K_SELECT = 10,
    K_FROM = 11,
    K_INNER = 12,
    K_JOIN = 13,
    K_ON = 14,
    K_WHERE = 15,
    K_GROUP = 16,
    K_NULL = 17,
    K_NOT = 18,
    K_BOOL = 19,
    K_INT64 = 20,
    K_FLOAT64 = 21,
    K_STRING = 22,
    K_TIMESTAMP_MICROS = 23,
    K_FLOAT_VECTOR = 24,
    K_TRUE = 25,
    K_FALSE = 26,
    K_CAST = 27,
    K_AND = 28,
    K_OR = 29,
    K_IS = 30,
    K_DISTINCT = 31,
    K_HAVING = 32,
    K_ORDER = 33,
    K_LIMIT = 34,
    K_OFFSET = 35,
    K_UNION = 36,
    K_WITH = 37,
    K_RECURSIVE = 38,
    K_OVER = 39,
    K_WINDOW = 40,
    K_LEFT = 41,
    K_RIGHT = 42,
    K_FULL = 43,
    K_OUTER = 44,
    K_CROSS = 45,
    K_NATURAL = 46,
    K_USING = 47,
    K_INSERT = 48,
    K_UPDATE = 49,
    K_DELETE = 50,
    K_DROP = 51,
    K_ALTER = 52,
    K_REPLACE = 53,
    LPAREN = 54,
    RPAREN = 55,
    COMMA = 56,
    SEMICOLON = 57,
    DOT = 58,
    PLUS = 59,
    MINUS = 60,
    STAR = 61,
    SLASH = 62,
    EQ = 63,
    NEQ = 64,
    LE = 65,
    LT = 66,
    GE = 67,
    GT = 68,
    NUMBER = 69,
    STRING_LITERAL = 70,
    QUOTED_IDENTIFIER = 71,
    IDENTIFIER = 72,
    LINE_COMMENT = 73,
    WHITESPACE = 74
  };

  explicit EventFlowSqlLexer(antlr4::CharStream* input);

  ~EventFlowSqlLexer() override;

  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.
};
