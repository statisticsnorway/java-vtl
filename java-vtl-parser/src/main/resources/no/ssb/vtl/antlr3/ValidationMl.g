grammar ValidationMl;

options {
  output = AST;
}

tokens {
  EVAL              = 'eval';
  PUT               = 'put';
  IF                = 'if';
  THEN              = 'then';
  ELSE              = 'else';
  ELSEIF            = 'elseif';
  BREAK             = 'break';
  BREAKDATE         = 'break_date';
  DIFFPERC          = 'diffperc';
  RELPERC           = 'relperc';
  EXKEY             = 'exkey';
  USING             = 'using';
  WITH              = 'with';
  CURRENT_DATE      = 'current_date';
  ON                = 'on';
  DROP              = 'drop';
  KEEP              = 'keep';
  CALC              = 'calc';
  ATTRCALC          = 'attrcalc';
  MATCHKEY          = 'matchkey';
  RENAME            = 'rename';
  AS                = 'as';
  AND               = 'and';
  OR                = 'or';
  XOR               = 'xor';
  NOT               = 'not';
  BETWEEN           = 'between';
  IN                = 'in';
  ISNULL            = 'isnull';
  EX                = 'ex';
  NEX               = 'nex';
  UNION             = 'union';
  DIFF              = 'symdiff';
  INTERSECT         = 'intersect';
  NOT_IN            = 'setdiff';
  KEYS              = 'keys';
  EQ                = '=';
  LT                = '<';
  GT                = '>';
  LE                = '<=';
  GE                = '>=';
  NE                = '<>';
  PLUS              = '+';
  MINUS             = '-';
  PLUSPLUS          = '++';
  MINUSMINUS        = '--';
  MULTMULT          = '**';
  DIVDIV            = '//';
  MULTIPLY          = '*';
  DIVIDE            = '/';
  PERCENT           = '%';
  ASSIGN            = ':=';
  CURRCHANGE        = 'currchange';
  CARTESIAN_PER     = ',';
  INTYEAR           = 'intyear';
  INTMONTH          = 'intmonth';
  INTDAY            = 'intday';
  TSUM              = 'tsum';
  TAVG              = 'tavg';
  TSTD              = 'tstd';
  TMEDIAN           = 'tmedian';
  TCOUNT            = 'tcount';
  TMIN              = 'tmin';
  TMAX              = 'tmax';
  PLUSFUN           = 'plus';
  MINUSFUN          = 'minus';
  PLUS2FUN          = 'plus2';
  MINUS2FUN         = 'minus2';
  MULTFUN           = 'mult';
  DIVFUN            = 'div';
  GET               = 'get';
  CHECK             = 'check';
  TIME_BEHAVIOR     = 'time_behavior';
  EXISTS_IN         = 'exists_in';
  EXISTS_IN_ALL     = 'exists_in_all';
  NOT_EXISTS_IN     = 'not_exists_in';
  NOT_EXISTS_IN_ALL = 'not_exists_in_all';
  DATASET_OVERLAP_TAG;
  DATASET_UMEETS_TAG;
  DATASET_KEYSET_TAG;
  DATASET_HMEETS_TAG;
  TO                = 'to';
  RETURN            = 'return';
  IMBALANCE         = 'imbalance';
  ERLEVEL           = 'erlevel';
  ERRORCODE         = 'errorcode';
  THRESHOLD         = 'threshold';
  ALL               = 'all';
  DATASET_LEN_TAG;
  DATASET_CONCAT_TAG;
  DATASET_TRIM_TAG;
  DATASET_LTRIM_TAG;
  DATASET_RTRIM_TAG;
  DATASET_UCASE_TAG;
  DATASET_CLAUSE_TAG;
  MERGE_ON          = 'merge on';
  AGGREGATE         = 'aggregate';
  COND              = 'cond';
  INBALANCE         = 'inbalance';
  ERLEVEL           = 'erlevel';
  ENDPERIOD         = 'ep';
  AVGPERIOD         = 'ap';
  FROM_CURR         = 'fromcurr';
  TO_CURR           = 'tocurr';
  TIMEFILTER        = 'timefilter';
  AS                = 'as';
  ORDER             = 'order';
  BY                = 'by';
  RANK              = 'rank';
  ASC               = 'asc';
  DESC              = 'desc';
  MIN               = 'min';
  MAX               = 'max';
  FIRST             = 'first';
  LAST              = 'last';
  INDEXOF           = 'indexof';
  EXCHECK           = 'excheck';
  COMPLCHECK        = 'complcheck';
  CASE              = 'case';
  ELSE              = 'else';
  ABS               = 'abs';
  KEY               = 'key';
  LN                = 'ln';
  LOG               = 'log';
  TRUNC             = 'trunc';
  ROUND             = 'round';
  POWER             = 'power';
  NROOT             = 'nroot';
  MOD               = 'mod';
  LEN               = 'length';
  CONCAT            = 'concat';
  TRIM              = 'trim';
  UCASE             = 'upper';
  LCASE             = 'lower';
  SUBSTR            = 'substr';
  SUM               = 'sum';
  AVG               = 'avg';
  STD               = 'std';
  MEDIAN            = 'median';
  COUNT             = 'count';
  COUNT_DISTINCT    = 'count_distinct';
  PERCENTILE        = 'percentile';
  DIMENSION         = 'IDENTIFIER';
  MEASURE           = 'MEASURE';
  ATTRIBUTE         = 'ATTRIBUTE';
  FILTER            = 'filter';
  INCLUDE           = 'include';
  EXCLUDE           = 'exclude';
  PCSFILTER         = 'pcsfilter';
  MERGE             = 'merge';
  LEFTC             = 'leftc';
  EXP               = 'exp';
  OVERLAP           = 'overlap';
  HMEETS            = 'hmeets';
  UMEETS            = 'umeets';
  ROLE              = 'role';
  VIRAL             = 'viral';
  CHECK             = 'check';
  SEVERITY          = 'severity';
  DISCREPANCY       = 'discrepancy';
  MATCHES_VALID     = 'is_valid';
  MATCHES_INVALID   = 'is_invalid';
  DATASET_LEVEL     = 'dataset_level';
  NODUPLICATES      = 'no_duplicates';
  MISSING           = 'missing';
  CHARSET_MATCH     = 'match_characters';
  CODELIST_MATCH    = 'match_values';
  TYPE              = 'datatype';
  CHARLENGTH        = 'charlength';
  NVL               = 'nvl';
  HIERARCHY         = 'hierarchy';
  ASSIGN_TAG;
  DATASET_OR_TAG;
  DATASET_XOR_TAG;
  DATASET_AND_TAG;
  DATASET_NOT_TAG;
  DATASET_BETWEEN_TAG;
  DATASET_NOT_BETWEEN_TAG;
  DATASET_MINUS_TAG;
  DATASET_MINUSMINUS_TAG;
  DATASET_PLUS_TAG;
  DATASET_PLUSPLUS_TAG;
  DATASET_MULTMULT_TAG;
  DATASET_DIVDIV_TAG;
  DATASET_PLUSFUN_TAG;
  DATASET_PLUS2FUN_TAG;
  DATASET_MINUSFUN_TAG;
  DATASET_MINUS2FUN_TAG;
  DATASET_DIVIDE_TAG;
  DATASET_MULTIPLY_TAG;
  DATASET_DIVIDEFUN_TAG;
  DATASET_MULTIPLYFUN_TAG;
  DATASET_PERCENT_TAG;
  DATASET_MINUS_UNARY_TAG;
  DATASET_PLUS_UNARY_TAG;
  DATASET_COMPARE_TAG;
  DATASET_EQ_TAG;
  DATASET_LT_TAG;
  DATASET_GT_TAG;
  DATASET_LE_TAG;
  DATASET_GE_TAG;
  DATASET_NE_TAG;
  DATASET_IN_TAG;
  DATASET_MIN_TAG;
  DATASET_MAX_TAG;
  DATASET_ABS_TAG;
  DATASET_EXP_TAG;
  DATASET_LN_TAG;
  DATASET_LOG_TAG;
  DATASET_MATCH_TAG;
  DATASET_TRUNC_TAG;
  DATASET_ROUND_TAG;
  DATASET_POWER_TAG;
  DATASET_NROOT_TAG;
  DATASET_CHECK_TAG;
  DATASET_CHECK_CONDITION_TAG;
  DATASET_CHECK_IMBALANCE_TAG;
  DATASET_CHECK_ERLEVEL_TAG;
  DATASET_NVL_TAG;
  DATASET_CHECK_ALL_TAG;
  DATASET_CHECK_THRESHOLD_TAG;
  DATASET_CHECK_ERRORCODE_TAG;
  DATASET_CURRCHANGE_TAG;
  DATASET_GET_TAG;
  DATASET_GET_DATASETID_TAG;
  DATASET_GET_DATASETAGGR_TAG;
  DATASET_GET_MERGE_CLAUSE_TAG;
  DATASET_GET_KEEP_CLAUSE_TAG;
  DATASET_GET_FILTER_CLAUSE_TAG;
  DATASET_GET_FILTERS_CLAUSE_TAG;
  DATASET_GET_PCSFILTER_CLAUSE_TAG;
  DATASET_GET_TIMEFILTER_CLAUSE_TAG;
  DATASET_GET_TIMEDELAY_CLAUSE_TAG;
  DATASET_EXCHECK_TAG;
  DATASET_COMPLCHECK_TAG;
  DATASET_EXCHECK_COMPL_KEYSET_TAG;
  DATASET_EXCHECK_COMPL_ERRORLEVEL_TAG;
  DATASET_CASE_TAG;
  DATASET_CASE_IF_TAG;
  DATASET_CASE_ELSE_TAG;
  DATASET_FILTER_CLAUSE_TAG;
  DATASET_FIRST_CLAUSE_TAG;
  DATASET_LAST_CLAUSE_TAG;
  DATASET_LAG_CLAUSE_TAG;
  DATASET_INCLUDE_CLAUSE_TAG;
  DATASET_EXCLUDE_CLAUSE_TAG;
  DATASET_RENAME_CLAUSE_TAG;
  DATASET_TOP_CLAUSE_TAG;
  DATASET_TOP_ORDERBY_CLAUSE_TAG;
  DATASET_RANK_CLAUSE_TAG;
  DATASET_RANK_ORDERBY_CLAUSE_TAG;
  DATASET_CALC_CLAUSE_TAG;
  DATASET_DROP_CLAUSE_TAG;
  DATASET_KEEP_CLAUSE_TAG;
  DATARIF_TIMEDELAY_CLAUSE_TAG;
  DATASET_USING_CLAUSE_TAG;
  DATASET_DOT_MEASURE_TAG;
  DATASET_AGGREGATION_MEASURE_TAG;
  DATASET_AGGREGATION_CLAUSE_TAG;
  DATASET_AGGREGATION_SUBCLAUSE_TAG;
  DATASET_MERGE_TAG;
  DATASET_LEFTC_TAG;
  DATASET_UNIONC_TAG;
  DATASET_INTERSECTC_TAG;
  DATASET_DIFFC_TAG;
  DATASET_INTERSECT_TAG;
  DATASET_NOT_IN_TAG;
  DATASET_UNION_TAG;
  DATASET_DIFF_TAG;
  DATASET_EXISTS_IN_TAG;
  DATASET_EXISTS_IN_ALL_TAG;
  DATASET_NOT_EXISTS_IN_TAG;
  DATASET_NOT_EXISTS_IN_ALL_TAG;
  DATASET_LCASE_TAG;
  DATASET_SUBSTR_TAG;
  DATASET_INDEXOF_TAG;
  DATASET_MOD_TAG;

  // validation ml tags
  DATASET_CHECK_TAG;
  DATASET_CHECK_CONDITION_TAG;
  DATASET_CHECK_DISCREPANCY_TAG;
  DATASET_CHECK_SEVERITY_TAG;
  DATASET_CHECK_LEVEL_TAG;
  DATASET_VALIDATE_CONDITIONTYPE_TAG;
  DATASET_INSET_TAG;
  DATASET_TYPE_TAG;
  DATASET_NODUPLICATES_TAG;
  DATASET_CHARSET_TAG;
  DATASET_CHARLENGTH_TAG;
  DATASET_INBETWEEN_TAG;
  RULE_TAG;
  SET_RANGE_TAG;
  SCALAR_OR_TAG;
  SCALAR_XOR_TAG;
  SCALAR_AND_TAG;
  SCALAR_NOT_TAG;
  SCALAR_PLUS_TAG;
  SCALAR_MINUS_TAG;
  SCALAR_MULTIPLY_TAG;
  SCALAR_DIVIDE_TAG;
  SCALAR_PERCENT_TAG;
  SCALAR_EQ_TAG;
  SCALAR_LT_TAG;
  SCALAR_GT_TAG;
  SCALAR_LE_TAG;
  SCALAR_GE_TAG;
  SCALAR_NE_TAG;
  SCALAR_MINUS_UNARY_TAG;
  SCALAR_BETWEEN_TAG;
  SCALAR_NOT_BETWEEN_TAG;
  SCALAR_IN_SET_TAG;
  SCALAR_NOT_IN_SET_TAG;
  SCALAR_EQ_CLAUSE_TAG;
  SCALAR_LT_CLAUSE_TAG;
  SCALAR_GT_CLAUSE_TAG;
  SCALAR_LE_CLAUSE_TAG;
  SCALAR_GE_CLAUSE_TAG;
  SCALAR_NE_CLAUSE_TAG;
  SCALAR_BETWEEN_CLAUSE_TAG;
  SCALAR_NOT_BETWEEN_CLAUSE_TAG;
  SCALAR_IN_SET_CLAUSE_TAG;
  SCALAR_NOT_IN_SET_CLAUSE_TAG;
  SCALAR_CASE_TAG;
  SCALAR_CASE_IF_TAG;
  SCALAR_CASE_ELSE_TAG;
  SCALAR_LEN_TAG;
  SCALAR_CONCAT_TAG;
  SCALAR_TRIM_TAG;
  SCALAR_LTRIM_TAG;
  SCALAR_RTRIM_TAG;
  SCALAR_UCASE_TAG;
  SCALAR_LCASE_TAG;
  SCALAR_SUBSTR_TAG;
  SCALAR_ROUND_TAG;
  SCALAR_TRUNC_TAG;
  SCALAR_LN_TAG;
  SCALAR_EXP_TAG;
  SCALAR_MOD_TAG;
  SCALAR_ABS_TAG;
  SCALAR_INTYEAR_TAG;
  SCALAR_INTMONTH_TAG;
  SCALAR_INTDAY_TAG;
  SCALAR_POWER_TAG;
  SCALAR_TIMEDELAY_CLAUSE;
  SET_UNION_TAG;
  SET_DIFF_TAG;
  SET_INTERSECT_TAG;
  AGGREGATE_SUM_TAG;
  AGGREGATE_AVG_TAG;
  AGGREGATE_MIN_TAG;
  AGGREGATE_MAX_TAG;
  AGGREGATE_STD_TAG;
  AGGREGATE_COUNT_TAG;
  AGGREGATE_PERCENTILE_TAG;
  QUANTILENUM_TAG;
  QUANTILETYPE_TAG;
  AGGREGATE_MEDIAN_TAG;
  LAGLIST_TAG;
  VAR_DROP_TAG;
  VAR_KEEP_TAG;
  VARPAIR_CLAUSE_TAG;
  VAR_RENAME_AS_TAG;
  VAR_RANK_AS_TAG;
  ORDERBY_ASC_TYPE_TAG;
  ORDERBY_DESC_TYPE_TAG;
  KEYSET_KEYS_TAG;
  KEYSET_VAR_TAG;
  KEYSET_VARS_TAG;
  KEYSET_VAR_SETID_TAG;
  KEYSET_VAR_LIST_TAG;
  KEYSET_VARPARAM_TAG;
  KEYSET_COMP_TAG;
  ROLE_DIMENSION_TAG;
  ROLE_ATTRIBUTE_TAG;
  ROLE_MEASURE_TAG;
  CALC_CLAUSE_TAG;
  DATASET_EX_TAG;
  DATASET_NEX_TAG;
  CURRENCY_TAG;
  DATASETID_TAG;
  DATASETCONTEXTID_TAG;
  VARID_TAG;
  SETID_TAG;
  COMPATIBILITYRULEID_TAG;
  DIMENSIONID_TAG;
  COMPONENTID_TAG;
  AGGREGATERULEID_TAG;
  INT_CONSTANT_TAG;
  STRING_CONSTANT_TAG;
  FLOAT_CONSTANT_TAG;
  BOOLEAN_CONSTANT_TAG;
  NULL_CONSTANT_TAG;
  JOIN_LEFTARGUMENT_TAG;
  JOIN_RIGHTARGUMENT_TAG;
  PARAM_ARGUMENT_TAG;
  JOIN_LEFTVAR_TAG;
  JOIN_RIGHTVAR_TAG;
  JOIN_RENAME_TAG;
  JOIN_CONDITION_TAG;
  DATASET_LIST_TAG;
  DATASET_EXKEY_TAG;
  PRESENCE_CLAUSE_TAG;
  KEYSET_LIST_TAG;
  IS_COMPL_LIST_TAG;
  DATASET_DIFFPERC_TAG;
  DATASET_RELPERC_TAG;
  STRICT_CONDITION_TAG;
  DATASET_A_TAG;
  DATASET_B_TAG;
  LOWPERC_TAG;
  HIGHPERC_TAG;
  DATASET_BREAK_TAG;
  BREAK_DATE_TAG;
  BREAK_EXPR_TAG;
  TOLERANCE_TAG;
  SYSTIMESTAMP_TAG;
}

@parser::header {
package no.ssb.antlr3;
}

@lexer::header {
package no.ssb.antlr3;
}

@lexer::members {
List<RecognitionException> exceptions = new ArrayList<RecognitionException>();

public List<RecognitionException> getExceptions() {
	return exceptions;
}

@Override
public void reportError(RecognitionException e) {
	super.reportError(e);
	exceptions.add(e);
}
}

@parser::members {
public static void main(String[] args) throws Exception {
	ValidationMlLexer lex = new ValidationMlLexer(new ANTLRFileStream(args[0]));
	CommonTokenStream tokens = new CommonTokenStream(lex);

	ValidationMlParser parser = new ValidationMlParser(tokens);

	try {
		parser.start();
	} catch (RecognitionException e) {
		e.printStackTrace();
	}
}

List<RecognitionException> exceptions = new ArrayList<RecognitionException>();

public List<RecognitionException> getExceptions() {
	return exceptions;
}

public void reportError(RecognitionException e) {
	super.reportError(e);
	exceptions.add(e);
}
}

start
  :
  statement* EOF
    ->
      ^(RULE_TAG statement*)
  ;

/* Assignment */

statement
  :
  (variableRef ASSIGN expr)
    ->
      ^(ASSIGN_TAG variableRef expr)
  ;

/* Conditional */

expr
  :
  exprOr
  | IF exprOr THEN exprOr (ELSEIF exprOr THEN exprOr)* (ELSE exprOr)*
  ;

/* Logical OR */

exprOr
  :
  (exprAnd
      -> exprAnd)
  (
    (
      (OR b1=exprAnd)
        ->
          ^(DATASET_OR_TAG $exprOr $b1)
    )
    |
    (
      (XOR b1=exprAnd)
        ->
          ^(DATASET_XOR_TAG $exprOr $b1)
    )
  )*
  ;

/* Logical AND */

exprAnd
  :
  (exprEq
      -> exprEq)
  (
    (AND b1=exprEq)
      ->
        ^(DATASET_AND_TAG $exprAnd $b1)
  )*
  ;

/* Equality, inequality */

exprEq
  :
  (exprExists
      -> exprExists)
  (
    (
      (
        EQ
        | NE
        | LE
        | GE
      )
      b2=exprExists
    )
      ->
        ^(DATASET_EQ_TAG $exprEq $b2)
  )*
  ;

/* Matching */

exprExists
  :
  (exprComp
      -> exprComp)
  (
    (
      (EXISTS_IN e=exprComp)
        ->
          ^(DATASET_EXISTS_IN_TAG $exprExists $e)
    )
    |
    (
      (EXISTS_IN_ALL e=exprComp)
        ->
          ^(DATASET_EXISTS_IN_ALL_TAG $exprExists $e)
    )
    |
    (
      (NOT_EXISTS_IN e=exprComp)
        ->
          ^(DATASET_NOT_EXISTS_IN_TAG $exprExists $e)
    )
    |
    (
      (NOT_EXISTS_IN_ALL e=exprComp)
        ->
          ^(DATASET_NOT_EXISTS_IN_ALL_TAG $exprExists $e)
    )
  )*
  ;

/* Comparison, range */

exprComp
  :
  (exprAdd
      -> exprAdd)
  (
    (
      (IN b2=setExpr)
        ->
          ^(DATASET_IN_TAG $exprComp $b2)
    )
    |
    (
      (NOT IN b2=setExpr)
        ->
          ^(DATASET_NOT_IN_TAG $exprComp $b2)
    )
    |
    (
      (
        (
          GT
          | LT
        )
        b2=exprAdd
      )
        ->
          ^(DATASET_COMPARE_TAG $exprComp $b2)
    )
    |
    (
      (BETWEEN b1=exprAdd AND b2=exprAdd)
        ->
          ^(DATASET_BETWEEN_TAG $exprComp $b1 $b2)
    )
    |
    (
      (NOT BETWEEN b1=exprAdd AND b2=exprAdd)
        ->
          ^(DATASET_NOT_BETWEEN_TAG $exprComp $b1 $b2)
    )
  )*
  ;

/* Addition, subtraction */

exprAdd
  :
  (exprMultiply
      -> exprMultiply)
  (
    (
      (
        (PLUSPLUS b1=exprMultiply)
          ->
            ^(DATASET_PLUSPLUS_TAG $exprAdd $b1)
      )
      |
      (
        (MINUSMINUS b1=exprMultiply)
          ->
            ^(DATASET_MINUSMINUS_TAG $exprAdd $b1)
      )
      |
      (
        (PLUS b1=exprMultiply)
          ->
            ^(DATASET_PLUS_TAG $exprAdd $b1)
      )
      |
      (
        (MINUS b1=exprMultiply)
          ->
            ^(DATASET_MINUS_TAG $exprAdd $b1)
      )
    )
  )*
  ;

/* Multiplication, division */

exprMultiply
  :
  (exprFactor
      -> exprFactor)
  (
    (
      (
        (MULTMULT b1=exprFactor)
          ->
            ^(DATASET_MULTMULT_TAG $exprMultiply $b1)
      )
      |
      (
        (DIVDIV b1=exprFactor)
          ->
            ^(DATASET_DIVDIV_TAG $exprMultiply $b1)
      )
      |
      (
        (MULTIPLY b1=exprFactor)
          ->
            ^(DATASET_MULTIPLY_TAG $exprMultiply $b1)
      )
      |
      (
        (DIVIDE b1=exprFactor)
          ->
            ^(DATASET_DIVIDE_TAG $exprMultiply $b1)
      )
      |
      (
        (PERCENT b1=exprFactor)
          ->
            ^(DATASET_PERCENT_TAG $exprMultiply $b1)
      )
    )
  )*
  ;

/* Unary plus, unary minus, not */

exprFactor
  :
  (exprMember
      -> exprMember)
  | (PLUS exprMember
      ->
        ^(DATASET_PLUS_UNARY_TAG exprMember))
  | (MINUS exprMember
      ->
        ^(DATASET_MINUS_UNARY_TAG exprMember))
  | NOT exprMember
  ;

/* Membership and getClauses */

exprMember
  :
  exprAtom ('[' datasetClause ']')?('#' componentID)? 
  ;

/* Functions */

exprAtom
  :
  ROUND '(' b1=expr ',' b4=INTEGER_CONSTANT ')'
    ->
      ^(DATASET_ROUND_TAG $b1 $b4)
  | (MIN '(' b1=expr ')'
      ->
        ^(DATASET_MIN_TAG $b1))
  | (MAX '(' b1=expr ')'
      ->
        ^(DATASET_MAX_TAG $b1))
  | (ABS '(' b1=expr ')'
      ->
        ^(DATASET_ABS_TAG $b1))
  | (EXP '(' b1=expr ')'
      ->
        ^(DATASET_EXP_TAG $b1))
  | (LN '(' b1=expr ')'
      ->
        ^(DATASET_LN_TAG $b1))
  | (LOG '(' b1=expr ',' b3=logBase ')'
      ->
        ^(DATASET_LOG_TAG $b1 $b3))
  | (TRUNC '(' b1=expr ',' b4=INTEGER_CONSTANT ')'
      ->
        ^(DATASET_TRUNC_TAG $b1 $b4))
  | (POWER '(' b1=expr ',' b3=powerExponent ')'
      ->
        ^(DATASET_POWER_TAG $b1 $b3))
  | (NROOT '(' b1=expr ',' b4=INTEGER_CONSTANT ')'
      ->
        ^(DATASET_NROOT_TAG $b1 $b4))
  | (LEN '(' expr ')'
      ->
        ^(DATASET_LEN_TAG expr))
  | (TRIM '(' expr ')'
      ->
        ^(DATASET_TRIM_TAG expr))
  | (UCASE '(' expr ')'
      ->
        ^(DATASET_UCASE_TAG expr))
  | (LCASE '(' expr ')'
      ->
        ^(DATASET_LCASE_TAG expr))
  |
  (
    SUBSTR '(' b1=expr ',' b2=scalarExpr (',' b3=scalarExpr)? ')'
      ->
        ^(DATASET_SUBSTR_TAG $b1 $b2 $b3?)
  )
  | (INDEXOF '(' b1=expr ',' str=STRING_CONSTANT ')'
      ->
        ^(DATASET_INDEXOF_TAG $b1 $str))
  | (MISSING '(' b1=expr ')'
      ->
        ^(DATASET_NODUPLICATES_TAG $b1))
  |
  (
    CHARSET_MATCH '(' o1=expr ',' str=STRING_CONSTANT (',' ALL)? ')'
      ->
        ^(DATASET_CHARSET_TAG $o1 $str)
  )
  |
  (
    CODELIST_MATCH '(' o1=expr ',' s1=setExpr (',' ALL)? ')'
      ->
        ^(DATASET_INSET_TAG $o1 $s1)
  )
  | (CHARLENGTH '(' o1=expr ')'
      ->
        ^(DATASET_CHARLENGTH_TAG $o1))
  | (TYPE '(' o1=expr ')' '=' o3=STRING_CONSTANT
      ->
        ^(
          DATASET_TYPE_TAG $o1
          ^(STRING_CONSTANT_TAG $o3)
         ))
  | INTERSECT '(' o1=expr ',' o2=expr ')'
    ->
      ^(DATASET_INTERSECT_TAG $o1 $o2)
  | UNION '(' o1=expr ',' o2=expr ')'
    ->
      ^(DATASET_UNION_TAG $o1 $o2)
  | DIFF '(' o1=expr ',' o2=expr ')'
    ->
      ^(DATASET_DIFF_TAG $o1 $o2)
  | NOT_IN '(' o1=expr ',' o2=expr ')'
    ->
      ^(DATASET_NOT_IN_TAG $o1 $o2)
  | ISNULL '(' o1=expr ')'
    ->
      ^(DATASET_NOT_IN_TAG $o1)
  | NVL '(' b1=expr ',' c=constant ')'
    ->
      ^(DATASET_NVL_TAG $b1 $c)
  | (MOD '(' b1=expr ',' b2=expr ')'
      ->
        ^(DATASET_MOD_TAG $b1 INTEGER_CONSTANT))
  | validationExpr
  | getExpr
  | variableRef
  | putExpr
  | evalExpr
  | mergeExpr
  | hierarchyExpr
  ;

/* Parentheses */

variableRef
  :
  '('! exprOr ')'!
  | varID
  | constant
  ; 

/* get */

getExpr
  :
  GET '(' persistentDatasetID (',' persistentDatasetID)* (',' keepClause)? (',' filterClause)? (',' aggregategetClause)? ')'
  ;

persistentDatasetID
  :
  STRING_CONSTANT
  ;

putExpr
  :
  PUT '(' expr ',' persistentDatasetID ')'
  ;

evalExpr
  :
  EVAL '(' STRING_CONSTANT (',' variableRef)* ',' persistentDatasetID ')'
  ;

validationExpr
  :
  CHECK '(' c1=exprOr (',' IMBALANCE '(' c2=exprOr ')')? (',' ERLEVEL '(' c3=exprOr ')')? (',' ERRORCODE '(' c4=constant ')')? (',' THRESHOLD '(' c5=constant ')')? (',' c6=ALL)? ')'
    ->
      ^(
        DATASET_CHECK_TAG
        ^(DATASET_CHECK_CONDITION_TAG $c1)
        ^(DATASET_CHECK_IMBALANCE_TAG $c2?)
        ^(DATASET_CHECK_ERLEVEL_TAG $c3?)
        ^(DATASET_CHECK_ERRORCODE_TAG $c4?)
        ^(DATASET_CHECK_THRESHOLD_TAG $c5?)
        ^(DATASET_CHECK_ALL_TAG $c6?)
       )
  ;

mergeExpr
  :
  MERGE '(' expr AS? STRING_CONSTANT (',' expr AS? STRING_CONSTANT)+ ',' ON '(' expr ')' ',' RETURN '(' (expr AS? STRING_CONSTANT) (',' expr AS? STRING_CONSTANT)+ ')' ')'
  ;

hierarchyExpr
  :
  HIERARCHY '(' expr ',' IDENTIFIER ','
  (
    STRING_CONSTANT
    | (mappingExpr (',' mappingExpr)* AS STRING_CONSTANT)
  )
  ',' BOOLEAN_CONSTANT (',' aggrParam)? ')'
  ;

mappingExpr
  :
  '(' constant ',' INTEGER_CONSTANT ','
  (
    PLUS
    | MINUS
  )
  ')' TO constant
  ;

aggrParam
  :
  'sum'
  | 'prod'
  ;

aggregategetClause
  :
  AGGREGATE '(' aggrFunction '(' scalarExpr ')' (',' aggrFunction '(' scalarExpr ')')* ')'
    ->
      ^(aggrFunction scalarExpr)+
  ;

aggregateClause
  :
  aggrFunctionClause (',' aggrFunctionClause)*
    ->
      ^(aggrFunctionClause)+
  ;

aggrFunctionClause
  :
  aggrFunction '(' scalarExpr ')'
    ->
      ^(aggrFunction scalarExpr)
  | percentileFunction
  ;

datasetIDGroup
  :
  varID (',' varID)*
    ->
      ^(varID)+
  ;

caseElseClause
  :
  (
    (',' ELSE exprAdd)
      ->
        ^(DATASET_CASE_ELSE_TAG exprAdd)
  )
  ;

caseCaseClause
  :
  (
    (exprOr ',' exprOr (',' exprOr ',' exprOr)*)
      ->
        ^(DATASET_CASE_IF_TAG exprOr exprOr)+
  )
  ;

getFiltersClause
  :
  (
    getFilterClause (',' getFilterClause)*
      ->
        ^(getFilterClause)+
  )
  ;

getFilterClause
  :
  (
    (FILTER? b1=bScalarExpr)
      ->
        ^(DATASET_GET_FILTER_CLAUSE_TAG $b1)
  )
  ;

datasetClause
  :
  (RENAME renameClause
      ->
        ^(DATASET_RENAME_CLAUSE_TAG renameClause))
  | aggrFilterClause
  | (calcClause
      ->
        ^(DATASET_CALC_CLAUSE_TAG calcClause))
  | (attrCalcClause
      ->
        ^(DATASET_CALC_CLAUSE_TAG attrCalcClause))
  | (keepClause
      ->
        ^(DATASET_KEEP_CLAUSE_TAG keepClause))
  | (dropClause
      ->
        ^(DATASET_DROP_CLAUSE_TAG dropClause))
  | (compareClause
      ->
        ^(DATASET_COMPARE_TAG compareClause))
  ;

aggrFilterClause
  :
  |
  (
    filterClause (',' keepClause ',' AGGREGATE aggregateClause)?
      ->
        ^(DATASET_KEEP_CLAUSE_TAG filterClause keepClause? aggregateClause?)
  )
  |
  (
    (
      keepClause
      | dropClause
    )
    ',' AGGREGATE aggregateClause
      ->
        ^(DATASET_KEEP_CLAUSE_TAG keepClause? dropClause? aggregateClause)
  )
  ;

filterClause
  :
  (
    (FILTER? b1=bScalarExpr)
      ->
        ^(DATASET_FILTER_CLAUSE_TAG $b1)
  )
  ;

ascdescClause
  :
  (ASC
      -> ORDERBY_ASC_TYPE_TAG)
  | (DESC
      -> ORDERBY_DESC_TYPE_TAG)
  ;

renameClause
  :
  (
    varID AS varID (ROLE varRole)? (',' varID AS varID (ROLE varRole)?)*
      ->
        ^(VAR_RENAME_AS_TAG varID varID)+
  )
  ;

aggrFunction
  :
  SUM
    -> AGGREGATE_SUM_TAG
  | AVG
    -> AGGREGATE_AVG_TAG
  | MIN
    -> AGGREGATE_MIN_TAG
  | MAX
    -> AGGREGATE_MAX_TAG
  | STD
    -> AGGREGATE_STD_TAG
  | COUNT
    -> AGGREGATE_COUNT_TAG
  | COUNT_DISTINCT
    -> AGGREGATE_COUNT_TAG
  | MEDIAN
    -> AGGREGATE_MEDIAN_TAG
  ;

percentileFunction
  :
  (PERCENTILE '(' a1=scalarExpr ',' a2=constant ')'
      ->
        ^(AGGREGATE_PERCENTILE_TAG $a1 $a2))
  ;

calcClause
  :
  (
    (CALC) calcClauseItem (',' calcClauseItem)*
      ->
        ^(calcClauseItem)+
  )
  ;

attrCalcClause
  :
  ATTRCALC scalarExpr AS STRING_CONSTANT (VIRAL)? (',' IDENTIFIER AS STRING_CONSTANT (VIRAL)?)*
  ;

calcClauseItem
  :
  (
    calcExpr AS STRING_CONSTANT (ROLE varRole (VIRAL)?)?
      ->
        ^(CALC_CLAUSE_TAG calcExpr varRole?)
  )
  ;

calcExpr
  :
  (t1=aggrFunction '(' s=scalarExpr ')'
      ->
        ^($s))
  | scalarExpr
  ;

dropClause
  :
  DROP '(' dropClauseItem (',' dropClauseItem)* ')'
    ->
      ^(dropClauseItem)+
  ;

dropClauseItem
  :
  (varID
      ->
        ^(VAR_DROP_TAG varID))
  ;

keepClause
  :
  KEEP '(' keepClauseItem (',' keepClauseItem)* ')'
    ->
      ^(keepClauseItem)+
  ;

keepClauseItem
  :
  (varID
      ->
        ^(VAR_KEEP_TAG varID))
  ;

compareClause
  :
  (b0=compOpScalarClause b1=constant
      ->
        ^($b0 $b1))
  ;

inBetweenClause
  :
  (
    (IN c2=setExpr)
      ->
        ^(SCALAR_IN_SET_CLAUSE_TAG $c2)
  )
  |
  (
    (BETWEEN a1=constant AND a2=constant)
      ->
        ^(SCALAR_BETWEEN_CLAUSE_TAG $a1 $a2)
  )
  |
  (
    (NOT IN c2=setExpr)
      ->
        ^(SCALAR_NOT_IN_SET_CLAUSE_TAG $c2)
  )
  |
  (
    (NOT BETWEEN a1=constant AND a2=constant)
      ->
        ^(SCALAR_NOT_BETWEEN_CLAUSE_TAG $a1 $a2)
  )
  ;

dimClause
  :
  | compareClause
  | inBetweenClause
  ;

varRole
  :
  (DIMENSION
      -> ROLE_DIMENSION_TAG)
  | (MEASURE
      -> ROLE_MEASURE_TAG)
  | (ATTRIBUTE
      -> ROLE_ATTRIBUTE_TAG)
  ;

bScalarExpr
  :
  (sExprOr
      -> sExprOr)
  (
    (
      (OR b1=sExprOr)
        ->
          ^(SCALAR_OR_TAG $bScalarExpr $b1)
    )
    |
    (
      (XOR b1=sExprOr)
        ->
          ^(SCALAR_XOR_TAG $bScalarExpr $b1)
    )
  )*
  ;

sExprOr
  :
  (sExprAnd
      -> sExprAnd)
  (
    (AND b1=sExprAnd)
      ->
        ^(SCALAR_AND_TAG $sExprOr $b1)
  )*
  ;

sExprAnd
  :
  (
    (NOT sExprPredicate)
      ->
        ^(SCALAR_NOT_TAG sExprPredicate)
  )
  | sExprPredicate
  ;

sExprPredicate
  :
  (scalarExpr
      -> scalarExpr)
  (
    (
      (compOpScalar b2=scalarExpr)
        ->
          ^(compOpScalar $sExprPredicate $b2)
    )
    |
    (
      (IN c2=setExpr)
        ->
          ^(SCALAR_IN_SET_TAG $sExprPredicate $c2)
    )
    |
    (
      (BETWEEN a1=scalarExpr AND a2=scalarExpr)
        ->
          ^(SCALAR_BETWEEN_TAG $sExprPredicate $a1 $a2)
    )
    |
    (
      (NOT IN c2=setExpr)
        ->
          ^(SCALAR_NOT_IN_SET_TAG $sExprPredicate $c2)
    )
    |
    (
      (NOT BETWEEN a1=scalarExpr AND a2=scalarExpr)
        ->
          ^(SCALAR_NOT_BETWEEN_TAG $sExprPredicate $a1 $a2)
    )
  )?
  ;

scalarExpr
  :
  (sExprAdd
      -> sExprAdd)
  (
    (
      (PLUS b2=sExprAdd)
        ->
          ^(SCALAR_PLUS_TAG $scalarExpr $b2)
    )
    |
    (
      (MINUS b2=sExprAdd)
        ->
          ^(SCALAR_MINUS_TAG $scalarExpr $b2)
    )
  )*
  ;

sExprAdd
  :
  (sExprFactor
      -> sExprFactor)
  (
    (
      (DIVIDE b2=sExprFactor)
        ->
          ^(SCALAR_DIVIDE_TAG $sExprAdd $b2)
    )
    |
    (
      (MULTIPLY b2=sExprFactor)
        ->
          ^(SCALAR_MULTIPLY_TAG $sExprAdd $b2)
    )
    |
    (
      (PERCENT b2=sExprFactor)
        ->
          ^(SCALAR_PERCENT_TAG $sExprAdd $b2)
    )
  )*
  ;

sExprFactor
  :
  (
    (sExprAtom
        -> sExprAtom)
    |
    (
      (PLUS sExprAtom)
        -> sExprAtom
    )
    |
    (
      (MINUS sExprAtom)
        ->
          ^(SCALAR_MINUS_UNARY_TAG sExprAtom)
    )
  )
  ;

sExprAtom
  :
  varID
  | constant
  | '('! bScalarExpr ')'!
  | (LEN '(' scalarExpr ')'
      ->
        ^(SCALAR_LEN_TAG scalarExpr))
  |
  (
    CONCAT '(' b1=scalarExpr (',' b2=scalarExpr)+ ')'
      ->
        ^(SCALAR_CONCAT_TAG $b1 $b2+)
  )
  | (TRIM '(' scalarExpr ')'
      ->
        ^(SCALAR_TRIM_TAG scalarExpr))
  | (UCASE '(' scalarExpr ')'
      ->
        ^(SCALAR_UCASE_TAG scalarExpr))
  | (LCASE '(' scalarExpr ')'
      ->
        ^(SCALAR_LCASE_TAG scalarExpr))
  | (SUBSTR '(' b1=scalarExpr ',' b2=scalarExpr ',' b3=scalarExpr ')'
      ->
        ^(SCALAR_SUBSTR_TAG $b1 $b2 $b3))
  | (ROUND '(' scalarExpr ',' INTEGER_CONSTANT ')'
      ->
        ^(SCALAR_ROUND_TAG scalarExpr INTEGER_CONSTANT))
  | (TRUNC '(' scalarExpr ')'
      ->
        ^(SCALAR_TRUNC_TAG scalarExpr))
  | (LN '(' scalarExpr ')'
      ->
        ^(SCALAR_LEN_TAG scalarExpr))
  | (EXP '(' scalarExpr ')'
      ->
        ^(SCALAR_EXP_TAG scalarExpr))
  | (MOD '(' scalarExpr ',' INTEGER_CONSTANT ')'
      ->
        ^(SCALAR_MOD_TAG scalarExpr INTEGER_CONSTANT))
  | (ABS '(' scalarExpr ')'
      ->
        ^(SCALAR_ABS_TAG scalarExpr))
  | (POWER '(' scalarExpr ',' powerExponent ')'
      ->
        ^(SCALAR_POWER_TAG scalarExpr powerExponent))
  | (sys='SYSTIMESTAMP')
    ->
      ^(SYSTIMESTAMP_TAG $sys)
  ;

componentID
  :
  v1=IDENTIFIER
    ->
      ^(COMPONENTID_TAG $v1)
  ;

compOpScalarClause
  :
  (EQ
      ->
        ^(SCALAR_EQ_CLAUSE_TAG))
  | (LT
      ->
        ^(SCALAR_LT_CLAUSE_TAG))
  | (GT
      ->
        ^(SCALAR_GT_CLAUSE_TAG))
  | (LE
      ->
        ^(SCALAR_LE_CLAUSE_TAG))
  | (GE
      ->
        ^(SCALAR_GE_CLAUSE_TAG))
  | (NE
      ->
        ^(SCALAR_NE_CLAUSE_TAG))
  ;

logBase
  :
  INTEGER_CONSTANT
    ->
      ^(INT_CONSTANT_TAG INTEGER_CONSTANT)
  ;

powerExponent
  :
  (
    (exponent
        -> exponent)
    |
    (
      (PLUS exponent)
        -> exponent
    )
    |
    (
      (MINUS exponent)
        ->
          ^(SCALAR_MINUS_UNARY_TAG exponent)
    )
  )
  ;

exponent
  :
  INTEGER_CONSTANT
    ->
      ^(INT_CONSTANT_TAG INTEGER_CONSTANT)
  | FLOAT_CONSTANT
    ->
      ^(FLOAT_CONSTANT_TAG FLOAT_CONSTANT)
  ;

setExpr
  :
  '('! constant (','! constant)* ')'!
  | UNION '(' setExpr (',' setExpr)+ ')'
    ->
      ^(SET_UNION_TAG setExpr setExpr+)
  | (DIFF '(' b1=setExpr ',' b2=setExpr ')'
      ->
        ^(SET_DIFF_TAG $b1 $b2))
  | (INTERSECT '(' b1=setExpr ',' b2=setExpr ')'
      ->
        ^(SET_INTERSECT_TAG $b1 $b2))
  ;

varID
  :
  IDENTIFIER
    ->
      ^(VARID_TAG IDENTIFIER)
  ;

compOp
  :
  (EQ
      ->
        ^(DATASET_EQ_TAG))
  | (LT
      ->
        ^(DATASET_LT_TAG))
  | (GT
      ->
        ^(DATASET_GT_TAG))
  | (LE
      ->
        ^(DATASET_LE_TAG))
  | (GE
      ->
        ^(DATASET_GE_TAG))
  | (NE
      ->
        ^(DATASET_NE_TAG))
  ;

compOpScalar
  :
  (EQ
      ->
        ^(SCALAR_EQ_TAG))
  | (LT
      ->
        ^(SCALAR_LT_TAG))
  | (GT
      ->
        ^(SCALAR_GT_TAG))
  | (LE
      ->
        ^(SCALAR_LE_TAG))
  | (GE
      ->
        ^(SCALAR_GE_TAG))
  | (NE
      ->
        ^(SCALAR_NE_TAG))
  ;

constant
  :
  INTEGER_CONSTANT
    ->
      ^(INT_CONSTANT_TAG INTEGER_CONSTANT)
  | FLOAT_CONSTANT
    ->
      ^(FLOAT_CONSTANT_TAG FLOAT_CONSTANT)
  | BOOLEAN_CONSTANT
    ->
      ^(BOOLEAN_CONSTANT_TAG BOOLEAN_CONSTANT)
  | STRING_CONSTANT
    ->
      ^(STRING_CONSTANT_TAG STRING_CONSTANT)
  | NULL_CONSTANT
    ->
      ^(NULL_CONSTANT_TAG NULL_CONSTANT)
  ;

INTEGER_CONSTANT
  :
  '0'..'9'+
  ;

FLOAT_CONSTANT
  :
  ('0'..'9')+ '.' ('0'..'9')* FLOATEXP?
  | ('0'..'9')+ FLOATEXP
  ;

fragment
FLOATEXP
  :
  (
    'e'
    | 'E'
  )
  (
    PLUS
    | MINUS
  )?
  ('0'..'9')+
  ;

TIME_CLAUSE
  :
  (
    'T'
    | 't'
  )
  ;

BOOLEAN_CONSTANT
  :
  'true'
  | 'false'
  ;

NULL_CONSTANT
  :
  'null'
  ;

STRING_CONSTANT
  :
  '"' (~'"')* '"'
  ;

IDENTIFIER
  :
  LETTER
  (
    LETTER
    | '_'
    | '0'..'9'
  )*
  ;
/*
	DIGITS0_9
		: '0'..'9'
		;
	*/


fragment
LETTER
  :
  'A'..'Z'
  | 'a'..'z'
  ;

WS
  :
  (
    ' '
    | '\r'
    | '\t'
    | '\u000C'
  )
  
  {
   $channel = HIDDEN;
  }
  ;

isCompl
  :
  ('Y')
  | ('N')
  ;

lhperc
  :
  (
    PLUS
    | MINUS
  )?
  FLOAT_CONSTANT
  | ('INF')
  |
  (
    PLUS
    | MINUS
  )?
  INTEGER_CONSTANT
  ;

EOL
  :
  (
    '\r'
    | '\n'
  )
  
  {
   $channel = HIDDEN;
  }
  ;

ML_COMMENT
  :
  '/*' (.)* '*/' 
                {
                 $channel = HIDDEN;
                }
  ;
