grammar EventFlowSql;

statement
    : (createTable | createView | selectQuery) SEMICOLON? EOF
    ;

createTable
    : K_CREATE K_TABLE identifier LPAREN (columnDefinition COMMA)*
      K_PRIMARY K_KEY LPAREN identifierList RPAREN (COMMA columnDefinition)*
      RPAREN (K_PARTITION K_BY LPAREN identifierList RPAREN)?
    ;

columnDefinition
    : identifier dataType (K_NULL | K_NOT K_NULL)?
    ;

dataType
    : K_BOOL | K_INT64 | K_FLOAT64 | K_STRING | K_TIMESTAMP_MICROS | K_FLOAT_VECTOR
    ;

identifierList
    : identifier (COMMA identifier)*
    ;

createView
    : K_CREATE K_MATERIALIZED K_VIEW identifier K_AS selectQuery
    ;

selectQuery
    : K_SELECT selectItem (COMMA selectItem)* K_FROM tableReference
      joinClause? (K_WHERE expression)? (K_GROUP K_BY expression (COMMA expression)*)?
    ;

selectItem
    : expression (K_AS identifier)?
    ;

tableReference
    : identifier (K_AS? identifier)?
    ;

joinClause
    : K_INNER? K_JOIN tableReference K_ON expression
    ;

expression
    : orExpression
    ;

orExpression
    : andExpression (K_OR andExpression)*
    ;

andExpression
    : notExpression (K_AND notExpression)*
    ;

notExpression
    : K_NOT* comparisonExpression
    ;

comparisonExpression
    : additiveExpression
      ((EQ | NEQ | LT | LE | GT | GE) additiveExpression | K_IS K_NOT? K_NULL)?
    ;

additiveExpression
    : multiplicativeExpression ((PLUS | MINUS) multiplicativeExpression)*
    ;

multiplicativeExpression
    : unaryExpression ((STAR | SLASH) unaryExpression)*
    ;

unaryExpression
    : (PLUS | MINUS)* primaryExpression
    ;

primaryExpression
    : LPAREN expression RPAREN
    | K_CAST LPAREN expression K_AS dataType RPAREN
    | identifier LPAREN (expression (COMMA expression)*)? RPAREN
    | identifier (DOT (identifier | STAR))?
    | NUMBER
    | STRING_LITERAL
    | K_TRUE
    | K_FALSE
    | K_NULL
    | STAR
    ;

identifier
    : IDENTIFIER | QUOTED_IDENTIFIER
    ;

K_CREATE: C R E A T E;
K_TABLE: T A B L E;
K_MATERIALIZED: M A T E R I A L I Z E D;
K_VIEW: V I E W;
K_AS: A S;
K_PRIMARY: P R I M A R Y;
K_KEY: K E Y;
K_PARTITION: P A R T I T I O N;
K_BY: B Y;
K_SELECT: S E L E C T;
K_FROM: F R O M;
K_INNER: I N N E R;
K_JOIN: J O I N;
K_ON: O N;
K_WHERE: W H E R E;
K_GROUP: G R O U P;
K_NULL: N U L L;
K_NOT: N O T;
K_BOOL: B O O L;
K_INT64: I N T '64';
K_FLOAT64: F L O A T '64';
K_STRING: S T R I N G;
K_TIMESTAMP_MICROS: T I M E S T A M P '_' M I C R O S;
K_FLOAT_VECTOR: F L O A T '_' V E C T O R;
K_TRUE: T R U E;
K_FALSE: F A L S E;
K_CAST: C A S T;
K_AND: A N D;
K_OR: O R;
K_IS: I S;

// Reserve unsupported clauses so a trailing clause cannot become a table alias.
K_DISTINCT: D I S T I N C T;
K_HAVING: H A V I N G;
K_ORDER: O R D E R;
K_LIMIT: L I M I T;
K_OFFSET: O F F S E T;
K_UNION: U N I O N;
K_WITH: W I T H;
K_RECURSIVE: R E C U R S I V E;
K_OVER: O V E R;
K_WINDOW: W I N D O W;
K_LEFT: L E F T;
K_RIGHT: R I G H T;
K_FULL: F U L L;
K_OUTER: O U T E R;
K_CROSS: C R O S S;
K_NATURAL: N A T U R A L;
K_USING: U S I N G;
K_INSERT: I N S E R T;
K_UPDATE: U P D A T E;
K_DELETE: D E L E T E;
K_DROP: D R O P;
K_ALTER: A L T E R;
K_REPLACE: R E P L A C E;

LPAREN: '(';
RPAREN: ')';
COMMA: ',';
SEMICOLON: ';';
DOT: '.';
PLUS: '+';
MINUS: '-';
STAR: '*';
SLASH: '/';
EQ: '=';
NEQ: '<>' | '!=';
LE: '<=';
LT: '<';
GE: '>=';
GT: '>';
NUMBER: DIGIT+ ('.' DIGIT*)? EXPONENT? | '.' DIGIT+ EXPONENT?;
STRING_LITERAL: '\'' ('\'\'' | ~'\'')* '\'';
QUOTED_IDENTIFIER: '"' ('""' | ~'"')+ '"';
IDENTIFIER: [a-zA-Z_\u0080-\u{10FFFF}] [a-zA-Z_0-9\u0080-\u{10FFFF}]*;
LINE_COMMENT: '--' ~[\r\n]* -> skip;
WHITESPACE: [ \t\r\n\f]+ -> skip;

fragment DIGIT: [0-9];
fragment EXPONENT: [eE] [+-]? DIGIT+;
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
