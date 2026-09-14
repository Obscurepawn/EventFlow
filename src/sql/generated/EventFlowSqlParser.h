
// Generated from EventFlowSql.g4 by ANTLR 4.10

#pragma once

#include "antlr4-runtime.h"

class EventFlowSqlParser : public antlr4::Parser {
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

  enum {
    RuleStatement = 0,
    RuleCreateTable = 1,
    RuleColumnDefinition = 2,
    RuleDataType = 3,
    RuleIdentifierList = 4,
    RuleCreateView = 5,
    RuleSelectQuery = 6,
    RuleSelectItem = 7,
    RuleTableReference = 8,
    RuleJoinClause = 9,
    RuleExpression = 10,
    RuleOrExpression = 11,
    RuleAndExpression = 12,
    RuleNotExpression = 13,
    RuleComparisonExpression = 14,
    RuleAdditiveExpression = 15,
    RuleMultiplicativeExpression = 16,
    RuleUnaryExpression = 17,
    RulePrimaryExpression = 18,
    RuleIdentifier = 19
  };

  explicit EventFlowSqlParser(antlr4::TokenStream* input);

  EventFlowSqlParser(antlr4::TokenStream* input,
                     const antlr4::atn::ParserATNSimulatorOptions& options);

  ~EventFlowSqlParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class StatementContext;
  class CreateTableContext;
  class ColumnDefinitionContext;
  class DataTypeContext;
  class IdentifierListContext;
  class CreateViewContext;
  class SelectQueryContext;
  class SelectItemContext;
  class TableReferenceContext;
  class JoinClauseContext;
  class ExpressionContext;
  class OrExpressionContext;
  class AndExpressionContext;
  class NotExpressionContext;
  class ComparisonExpressionContext;
  class AdditiveExpressionContext;
  class MultiplicativeExpressionContext;
  class UnaryExpressionContext;
  class PrimaryExpressionContext;
  class IdentifierContext;

  class StatementContext : public antlr4::ParserRuleContext {
  public:
    StatementContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EOF();
    CreateTableContext* createTable();
    CreateViewContext* createView();
    SelectQueryContext* selectQuery();
    antlr4::tree::TerminalNode* SEMICOLON();
  };

  StatementContext* statement();

  class CreateTableContext : public antlr4::ParserRuleContext {
  public:
    CreateTableContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* K_CREATE();
    antlr4::tree::TerminalNode* K_TABLE();
    IdentifierContext* identifier();
    std::vector<antlr4::tree::TerminalNode*> LPAREN();
    antlr4::tree::TerminalNode* LPAREN(size_t i);
    antlr4::tree::TerminalNode* K_PRIMARY();
    antlr4::tree::TerminalNode* K_KEY();
    std::vector<IdentifierListContext*> identifierList();
    IdentifierListContext* identifierList(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RPAREN();
    antlr4::tree::TerminalNode* RPAREN(size_t i);
    std::vector<ColumnDefinitionContext*> columnDefinition();
    ColumnDefinitionContext* columnDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* K_PARTITION();
    antlr4::tree::TerminalNode* K_BY();
  };

  CreateTableContext* createTable();

  class ColumnDefinitionContext : public antlr4::ParserRuleContext {
  public:
    ColumnDefinitionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    DataTypeContext* dataType();
    antlr4::tree::TerminalNode* K_NULL();
    antlr4::tree::TerminalNode* K_NOT();
  };

  ColumnDefinitionContext* columnDefinition();

  class DataTypeContext : public antlr4::ParserRuleContext {
  public:
    DataTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* K_BOOL();
    antlr4::tree::TerminalNode* K_INT64();
    antlr4::tree::TerminalNode* K_FLOAT64();
    antlr4::tree::TerminalNode* K_STRING();
    antlr4::tree::TerminalNode* K_TIMESTAMP_MICROS();
    antlr4::tree::TerminalNode* K_FLOAT_VECTOR();
  };

  DataTypeContext* dataType();

  class IdentifierListContext : public antlr4::ParserRuleContext {
  public:
    IdentifierListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
  };

  IdentifierListContext* identifierList();

  class CreateViewContext : public antlr4::ParserRuleContext {
  public:
    CreateViewContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* K_CREATE();
    antlr4::tree::TerminalNode* K_MATERIALIZED();
    antlr4::tree::TerminalNode* K_VIEW();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* K_AS();
    SelectQueryContext* selectQuery();
  };

  CreateViewContext* createView();

  class SelectQueryContext : public antlr4::ParserRuleContext {
  public:
    SelectQueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* K_SELECT();
    std::vector<SelectItemContext*> selectItem();
    SelectItemContext* selectItem(size_t i);
    antlr4::tree::TerminalNode* K_FROM();
    TableReferenceContext* tableReference();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    JoinClauseContext* joinClause();
    antlr4::tree::TerminalNode* K_WHERE();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* K_GROUP();
    antlr4::tree::TerminalNode* K_BY();
  };

  SelectQueryContext* selectQuery();

  class SelectItemContext : public antlr4::ParserRuleContext {
  public:
    SelectItemContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* K_AS();
    IdentifierContext* identifier();
  };

  SelectItemContext* selectItem();

  class TableReferenceContext : public antlr4::ParserRuleContext {
  public:
    TableReferenceContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode* K_AS();
  };

  TableReferenceContext* tableReference();

  class JoinClauseContext : public antlr4::ParserRuleContext {
  public:
    JoinClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* K_JOIN();
    TableReferenceContext* tableReference();
    antlr4::tree::TerminalNode* K_ON();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* K_INNER();
  };

  JoinClauseContext* joinClause();

  class ExpressionContext : public antlr4::ParserRuleContext {
  public:
    ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OrExpressionContext* orExpression();
  };

  ExpressionContext* expression();

  class OrExpressionContext : public antlr4::ParserRuleContext {
  public:
    OrExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AndExpressionContext*> andExpression();
    AndExpressionContext* andExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> K_OR();
    antlr4::tree::TerminalNode* K_OR(size_t i);
  };

  OrExpressionContext* orExpression();

  class AndExpressionContext : public antlr4::ParserRuleContext {
  public:
    AndExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NotExpressionContext*> notExpression();
    NotExpressionContext* notExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> K_AND();
    antlr4::tree::TerminalNode* K_AND(size_t i);
  };

  AndExpressionContext* andExpression();

  class NotExpressionContext : public antlr4::ParserRuleContext {
  public:
    NotExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ComparisonExpressionContext* comparisonExpression();
    std::vector<antlr4::tree::TerminalNode*> K_NOT();
    antlr4::tree::TerminalNode* K_NOT(size_t i);
  };

  NotExpressionContext* notExpression();

  class ComparisonExpressionContext : public antlr4::ParserRuleContext {
  public:
    ComparisonExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AdditiveExpressionContext*> additiveExpression();
    AdditiveExpressionContext* additiveExpression(size_t i);
    antlr4::tree::TerminalNode* K_IS();
    antlr4::tree::TerminalNode* K_NULL();
    antlr4::tree::TerminalNode* EQ();
    antlr4::tree::TerminalNode* NEQ();
    antlr4::tree::TerminalNode* LT();
    antlr4::tree::TerminalNode* LE();
    antlr4::tree::TerminalNode* GT();
    antlr4::tree::TerminalNode* GE();
    antlr4::tree::TerminalNode* K_NOT();
  };

  ComparisonExpressionContext* comparisonExpression();

  class AdditiveExpressionContext : public antlr4::ParserRuleContext {
  public:
    AdditiveExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MultiplicativeExpressionContext*> multiplicativeExpression();
    MultiplicativeExpressionContext* multiplicativeExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> PLUS();
    antlr4::tree::TerminalNode* PLUS(size_t i);
    std::vector<antlr4::tree::TerminalNode*> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
  };

  AdditiveExpressionContext* additiveExpression();

  class MultiplicativeExpressionContext : public antlr4::ParserRuleContext {
  public:
    MultiplicativeExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UnaryExpressionContext*> unaryExpression();
    UnaryExpressionContext* unaryExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> STAR();
    antlr4::tree::TerminalNode* STAR(size_t i);
    std::vector<antlr4::tree::TerminalNode*> SLASH();
    antlr4::tree::TerminalNode* SLASH(size_t i);
  };

  MultiplicativeExpressionContext* multiplicativeExpression();

  class UnaryExpressionContext : public antlr4::ParserRuleContext {
  public:
    UnaryExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PrimaryExpressionContext* primaryExpression();
    std::vector<antlr4::tree::TerminalNode*> PLUS();
    antlr4::tree::TerminalNode* PLUS(size_t i);
    std::vector<antlr4::tree::TerminalNode*> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
  };

  UnaryExpressionContext* unaryExpression();

  class PrimaryExpressionContext : public antlr4::ParserRuleContext {
  public:
    PrimaryExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LPAREN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* RPAREN();
    antlr4::tree::TerminalNode* K_CAST();
    antlr4::tree::TerminalNode* K_AS();
    DataTypeContext* dataType();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* DOT();
    antlr4::tree::TerminalNode* STAR();
    antlr4::tree::TerminalNode* NUMBER();
    antlr4::tree::TerminalNode* STRING_LITERAL();
    antlr4::tree::TerminalNode* K_TRUE();
    antlr4::tree::TerminalNode* K_FALSE();
    antlr4::tree::TerminalNode* K_NULL();
  };

  PrimaryExpressionContext* primaryExpression();

  class IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* IDENTIFIER();
    antlr4::tree::TerminalNode* QUOTED_IDENTIFIER();
  };

  IdentifierContext* identifier();

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};
