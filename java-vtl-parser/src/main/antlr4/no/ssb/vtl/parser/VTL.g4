/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
grammar VTL;
start : statement+ EOF;

statement : assignment ;

assignment : variable ASSIGNMENT ( expression | datasetExpression ) ;

repeat : 'foreach' identifiers=variableList 'in' datasets=variableList 'do' statement+ 'done';
variableList : variable (COMMA variable)*;


functionCall       : nvlFunction // TODO: Create one rule per function?
                   | nativeFunctionCall
                   | userFunctionCall ;

userFunctionCall   : functionName=REG_IDENTIFIER LPAR functionParameters? RPAR ;
nativeFunctionCall : functionName=NATIVE_FUNCTIONS LPAR functionParameters? RPAR ;

functionParameters : namedExpression ( COMMA namedExpression)*
                   | expression ( COMMA expression )*
                   | expression ( COMMA expression )* COMMA namedExpression ( COMMA namedExpression)* ;

namedExpression     : name=REG_IDENTIFIER COLON expression ;

// Expressions
expression : LPAR expression RPAR                                               # precedenceExpr
           | functionCall                                                       # functionExpr

           | op=(NOT | PLUS | MINUS) right=expression                           # unaryExpr
           | left=expression op=(ISNULL|ISNOTNULL)                              # postfixExpr
           | <assoc=right> left=expression op=CONCAT right=expression           # binaryExpr
           | left=expression op=(MUL | DIV) right=expression                    # arithmeticExpr
           | left=expression op=(PLUS | MINUS) right=expression                 # arithmeticExpr
           | left=expression op=(LE | LT | GE | GT | EQ | NE) right=expression  # binaryExpr
           | left=expression op=AND right=expression                            # binaryExpr
           | left=expression op=(OR|XOR) right=expression                       # binaryExpr

           | ifThenElseExpression                                               # ifExpr
           | membershipExpression                                               # membershipExpr
           | variable                                                           # variableExpr
           | literal                                                            # literalExpr
           ;

membershipExpression : left=variable op=MEMBERSHIP right=variable ;

// TODO: Rename to variableName.
variable : ( ESCAPED_IDENTIFIER | REG_IDENTIFIER ) ;

// Literal.
literal : nullLiteral
         | booleanLiteral
         | dateLiteral
         | integerLiteral
         | floatLiteral
         | stringLiteral ;

nullLiteral     : NULL ;
booleanLiteral  : BOOLEAN_LITERAL ;
dateLiteral     : DATE_LITERAL ;
integerLiteral  : INTEGER_LITERAL ;
floatLiteral    : FLOAT_LITERAL ;
stringLiteral   : STRING_LITERAL ;

datasetExpression : <assoc=right>datasetExpression clauseExpression     #withClause
                  | hierarchyExpression                                 #withHierarchy
                  | relationalExpression                                #withRelational
                  | function                                            #withFunction
                  | variable                                            #withAtom
                  | repeat                                              #withRepeat
                  ;

hierarchyExpression : 'hierarchy' '(' variable ',' variableExpression ',' hierarchyReference ',' BOOLEAN_LITERAL ( ',' ('sum' | 'prod') )? ')' ;
hierarchyReference : variableExpression ;

// TODO: Move to expression.
function : getFunction               #withGet
         | putFunction               #withPut
         | checkFunction             #withCheck
         | aggregationFunction       #withAggregation
         ;

getFunction : 'get' LPAR stringLiteral RPAR;
putFunction : 'put' LPAR stringLiteral RPAR;

aggregationFunction
       : 'sum' '(' variableExpression ')' aggregationParams       #aggregateSum
       | 'avg' '(' variableExpression ')' aggregationParams       #aggregateAvg
       ;

aggregationParams: aggregationClause=(GROUP_BY|ALONG) variableExpression (',' variableExpression)*;

ALONG : 'along' ;
GROUP_BY : 'group by' ;

checkFunction : 'check' '(' checkParam ')';

checkParam : variableExpression (',' checkRows)? (',' checkColumns)? (',' 'errorcode' '(' errorCode ')' )? (',' 'errorlevel' '=' '(' errorLevel ')' )?;

checkRows : ( 'not_valid' | 'valid' | 'all' ) ;
checkColumns : ( 'measures' | 'condition' ) ;
errorCode : stringLiteral ;
errorLevel : integerLiteral ;

variableExpression : membershipExpression | variable ;

clauseExpression      : '[' clause ']' ;

clause       : 'rename' renameParam (',' renameParam)*     #renameClause
             | filter     #filterClause
             | keep       #keepClause
             | calc       #calcClause
             | attrcalc   #attrcalcClause
             | aggregate  #aggregateClause
             ;

// [ rename component as string,
//          component as string role = IDENTIFIER,
//          component as string role = MEASURE,
//          component as string role = ATTRIBUTE
// ]
renameParam : from=variable 'as' to=variable ( ROLE role=componentRole )? ;

filter      : 'filter' expression ;

keep        : 'keep' variable ( ',' variable )* ;

calc        : 'calc' ;

attrcalc    : 'attrcalc' ;

aggregate   : 'aggregate' ;

ASSIGNMENT : ':=' ;

/* Operators */

MEMBERSHIP : '.' ;

CONCAT : '||' ;
EQ : '='  ;
NE : '<>' ;
LE : '<=' ;
LT : '<'  ;
GE : '>=' ;
GT : '>'  ;

AND : 'and' ;
OR  : 'or' ;
XOR : 'xor' ;
NOT : 'not' ;
ISNULL : 'is null' ;
ISNOTNULL : 'is not null' ;

/* Core functions */

nvlFunction : 'nvl' LPAR expression COMMA expression RPAR ;

ifThenElseExpression: 'if' ifBody ('elseif' ifBody)* ('else' ifBodyExpression) ;

ifBody : ifBodyExpression 'then' ifBodyExpression ;

ifBodyExpression : ifThenElseExpression {notifyErrorListeners("value cannot be another if-then-else expression");}
                 | expression ;

NATIVE_FUNCTIONS : (NUMERIC_FUNCTIONS | STRING_FUNCTIONS | FUNC_ISNULL) ;

FUNC_ISNULL : 'isnull' ;

// Numeric
fragment NUMERIC_FUNCTIONS : ( FUNC_ROUND | FUNC_CEIL | FUNC_FLOOR | FUNC_ABS |
                      FUNC_TRUNC | FUNC_EXP  | FUNC_LN    | FUNC_LOG |
                      FUNC_POWER | FUNC_SQRT | FUNC_NROOT | FUNC_MOD |
                      FUNC_LISTSUM
                    ) ;

FUNC_ROUND  : 'round' ;
FUNC_CEIL   : 'ceil' ;
FUNC_FLOOR  : 'floor' ;
FUNC_ABS    : 'abs' ;
FUNC_TRUNC  : 'trunc' ;
FUNC_EXP    : 'exp' ;
FUNC_LN     : 'ln' ;
FUNC_LOG    : 'log' ;
FUNC_POWER  : 'power' ;
FUNC_SQRT   : 'sqrt' ;
FUNC_NROOT  : 'nroot' ;
FUNC_MOD    : 'mod' ;
FUNC_LISTSUM: 'listsum' ;

// String
fragment STRING_FUNCTIONS : ( FUNC_LENGTH | FUNC_TRIM  | FUNC_LTRIM  | FUNC_RTRIM |
                     FUNC_UPPER  | FUNC_LOWER | FUNC_SUBSTR | FUNC_INSTR
                   | FUNC_D_F_S
                   | FUNC_I_F_S
                   | FUNC_F_F_S
                   | FUNC_S_F_N
                   | FUNC_REPLACE
                   ) ;

FUNC_LENGTH  : 'length' ;
FUNC_TRIM    : 'trim' ;
FUNC_LTRIM   : 'ltrim' ;
FUNC_RTRIM   : 'rtrim' ;
FUNC_UPPER   : 'upper' ;
FUNC_LOWER   : 'lower' ;
FUNC_SUBSTR  : 'substr' ;
FUNC_INSTR   : 'instr' ;
FUNC_D_F_S   : 'date_from_string' ;
FUNC_I_F_S   : 'integer_from_string' ;
FUNC_F_F_S   : 'float_from_string' ;
FUNC_S_F_N   : 'string_from_number' ;
FUNC_REPLACE : 'replace' ;

relationalExpression : unionExpression | joinExpression ;

unionExpression : 'union' '(' datasetExpression (',' datasetExpression )* ')' ;

joinExpression : '[' joinDefinition ']' '{' joinBody '}';

joinDefinition : type=( INNER | OUTER | CROSS )? datasets=joinDefinitionVariables ( 'on' identifiers=joinDefinitionVariables)? ;
joinDefinitionVariables : variable (',' variable )* ;

joinBody : joinClause (',' joinClause)* ;

joinClause : joinAssignment                     # joinCalcClause
           | joinDropExpression                 # joinDropClause
           | joinKeepExpression                 # joinKeepClause
           | joinRenameExpression               # joinRenameClause
           | joinFilterExpression               # joinFilterClause
           | joinFoldExpression                 # joinFoldClause
           | joinUnfoldExpression               # joinUnfoldClause
           ;

joinAssignment : implicit=IMPLICIT? role=componentRole? variable ASSIGNMENT expression ;

// TODO: The spec writes examples with parentheses, but it seems unecessary to me.
// TODO: The spec is unclear regarding types of the elements, we support only strings ATM.
joinFoldExpression      : 'fold' variableExpression (',' variableExpression)* 'to' dimension=variable ',' measure=variable ;

// TODO: variableName instead of STRING?
joinUnfoldExpression    : 'unfold' dimension=variableExpression ',' measure=variableExpression 'to' stringLiteral (',' stringLiteral)* ;

// Drop clause
joinDropExpression : 'drop' variableExpression (',' variableExpression)* ;

// Keep clause
joinKeepExpression : 'keep' variableExpression (',' variableExpression)* ;

// Rename clause
joinRenameExpression : 'rename' joinRenameParameter (',' joinRenameParameter)* ;
joinRenameParameter  : from=variableExpression 'to' to=variable ;

// Filter clause
joinFilterExpression : 'filter' expression ;

componentRole : role=(IDENTIFIER | MEASURE | ATTRIBUTE);

INNER : 'inner' ;
OUTER : 'outer' ;
CROSS : 'cross' ;

ROLE : 'role' ;

// "2000-01-01T00:00:00.000Z"
DATE_LITERAL : FULL_DATE 'T' FULL_TIME ('Z' | OFFSET ) ;
fragment FULL_DATE : DIGIT4 '-' DIGIT2 '-' DIGIT2 ;
fragment FULL_TIME : DIGIT2 ':' DIGIT2 ':' DIGIT2 ( '.' DIGIT+ )? ;
fragment OFFSET : ('+' | '-' ) DIGIT+ ':' DIGIT+ ;
fragment DIGIT4 : DIGIT2 DIGIT2 ;
fragment DIGIT2 : DIGIT DIGIT ;

INTEGER_LITERAL  : (PLUS|MINUS)?DIGIT+;

BOOLEAN_LITERAL  : 'true' | 'false' ;

STRING_LITERAL   :'"' (ESCAPED_QUOTE|~'"')* '"';
fragment ESCAPED_QUOTE : '""';

FLOAT_LITERAL    : (PLUS|MINUS)?(DIGIT)+ '.' (DIGIT)* FLOATEXP?
         | (PLUS|MINUS)?(DIGIT)+ FLOATEXP
         ;

NULL     : 'null';

IMPLICIT : 'implicit' ;

IDENTIFIER : 'identifier' | 'IDENTIFIER' ;
MEASURE : 'measure' | 'MEASURE' ;
ATTRIBUTE : 'attribute' | 'ATTRIBUTE'  ;

//regular identifiers start with a (lowercase or uppercase) English alphabet letter, followed by zero or more letters, decimal digits, or underscores
REG_IDENTIFIER : LETTER(LETTER|'_'|DIGIT)* ;
//VTL 1.1 allows us to escape the limitations imposed on regular identifiers by enclosing them in single quotes (apostrophes).
ESCAPED_IDENTIFIER:  QUOTE (~['\r\n] | '\'\'')+ QUOTE;

fragment QUOTE : '\'';

MUL : '*' ;
DIV : '/' ;
PLUS : '+';
MINUS : '-';

LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;

fragment DIGIT    : '0'..'9' ;
fragment FLOATEXP : ('e'|'E')(PLUS|MINUS)?('0'..'9')+;
fragment LETTER   : 'A'..'Z' | 'a'..'z';

WS : [ \n\r\t\u000C] -> skip ;
COMMENT : '/*' .*? '*/' -> skip;
LINE_COMMENT : LineComment -> skip;

fragment LineComment
   : '//' ~ [\r\n]*
   ;

