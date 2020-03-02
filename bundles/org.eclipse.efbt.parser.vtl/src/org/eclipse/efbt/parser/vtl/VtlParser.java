/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Neil Mackenzie - initial API and implementation
 *  */
package org.eclipse.efbt.parser.vtl;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
/**
 * 
 *@author Neil Mackenzie
 *
 */
public class VtlParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, ASSIGN=19, MEMBERSHIP=20, EVAL=21, IF=22, THEN=23, ELSE=24, 
		USING=25, WITH=26, CURRENT_DATE=27, ON=28, DROP=29, KEEP=30, CALC=31, 
		ATTRCALC=32, RENAME=33, AS=34, AND=35, OR=36, XOR=37, NOT=38, BETWEEN=39, 
		IN=40, NOT_IN=41, ISNULL=42, EX=43, UNION=44, DIFF=45, SYMDIFF=46, INTERSECT=47, 
		KEYS=48, CARTESIAN_PER=49, INTYEAR=50, INTMONTH=51, INTDAY=52, CHECK=53, 
		EXISTS_IN=54, TO=55, RETURN=56, IMBALANCE=57, ERRORCODE=58, ALL=59, AGGREGATE=60, 
		ERRORLEVEL=61, ORDER=62, BY=63, RANK=64, ASC=65, DESC=66, MIN=67, MAX=68, 
		FIRST=69, LAST=70, INDEXOF=71, ABS=72, KEY=73, LN=74, LOG=75, TRUNC=76, 
		ROUND=77, POWER=78, MOD=79, LEN=80, CONCAT=81, TRIM=82, UCASE=83, LCASE=84, 
		SUBSTR=85, SUM=86, AVG=87, MEDIAN=88, COUNT=89, DIMENSION=90, MEASURE=91, 
		ATTRIBUTE=92, FILTER=93, MERGE=94, EXP=95, ROLE=96, VIRAL=97, CHARSET_MATCH=98, 
		TYPE=99, NVL=100, HIERARCHY=101, OPTIONAL=102, INVALID=103, VALUE_DOMAIN=104, 
		VARIABLE=105, DATA=106, STRUCTURE=107, DATASET=108, OPERATOR=109, DEFINE=110, 
		PUT_SYMBOL=111, DATAPOINT=112, HIERARCHICAL=113, RULESET=114, RULE=115, 
		END=116, ALTER_DATASET=117, LTRIM=118, RTRIM=119, INSTR=120, REPLACE=121, 
		CEIL=122, FLOOR=123, SQRT=124, ANY=125, SETDIFF=126, STDDEV_POP=127, STDDEV_SAMP=128, 
		VAR_POP=129, VAR_SAMP=130, GROUP=131, EXCEPT=132, HAVING=133, FIRST_VALUE=134, 
		LAST_VALUE=135, LAG=136, LEAD=137, RATIO_TO_REPORT=138, OVER=139, PRECEDING=140, 
		FOLLOWING=141, UNBOUNDED=142, PARTITION=143, ROWS=144, RANGE=145, CURRENT=146, 
		VALID=147, FILL_TIME_SERIES=148, FLOW_TO_STOCK=149, STOCK_TO_FLOW=150, 
		TIMESHIFT=151, MEASURES=152, NO_MEASURES=153, CONDITION=154, BOOLEAN=155, 
		DATE=156, TIME_PERIOD=157, NUMBER=158, STRING=159, INTEGER=160, FLOAT=161, 
		LIST=162, RECORD=163, RESTRICT=164, YYYY=165, MM=166, DD=167, MAX_LENGTH=168, 
		REGEXP=169, IS=170, WHEN=171, FROM=172, AGGREGATES=173, POINTS=174, POINT=175, 
		TOTAL=176, PARTIAL=177, ALWAYS=178, INNER_JOIN=179, LEFT_JOIN=180, CROSS_JOIN=181, 
		FULL_JOIN=182, MAPS_FROM=183, MAPS_TO=184, MAP_TO=185, MAP_FROM=186, RETURNS=187, 
		PIVOT=188, UNPIVOT=189, SUBSPACE=190, APPLY=191, CONDITIONED=192, PERIOD_INDICATOR=193, 
		SINGLE=194, DURATION=195, TIME_AGG=196, UNIT=197, VALUE=198, VALUEDOMAINS=199, 
		VARIABLES=200, INPUT=201, OUTPUT=202, CAST=203, RULE_PRIORITY=204, DATASET_PRIORITY=205, 
		DEFAULT=206, CHECK_DATAPOINT=207, CHECK_HIERARCHY=208, COMPUTED=209, NON_NULL=210, 
		NON_ZERO=211, PARTIAL_NULL=212, PARTIAL_ZERO=213, ALWAYS_NULL=214, ALWAYS_ZERO=215, 
		COMPONENTS=216, ALL_MEASURES=217, SCALAR=218, COMPONENT=219, DATAPOINT_ON_VD=220, 
		DATAPOINT_ON_VAR=221, HIERARCHICAL_ON_VD=222, HIERARCHICAL_ON_VAR=223, 
		SET=224, LANGUAGE=225, INTEGER_CONSTANT=226, POSITIVE_CONSTANT=227, NEGATIVE_CONSTANT=228, 
		FLOAT_CONSTANT=229, BOOLEAN_CONSTANT=230, NULL_CONSTANT=231, STRING_CONSTANT=232, 
		IDENTIFIER=233, DIGITS0_9=234, MONTH=235, DAY=236, YEAR=237, WEEK=238, 
		HOURS=239, MINUTES=240, SECONDS=241, DATE_FORMAT=242, TIME_FORMAT=243, 
		TIME_UNIT=244, TIME=245, WS=246, EOL=247, ML_COMMENT=248, SL_COMMENT=249, 
		COMPARISON_OP=250, FREQUENCY=251;
	public static final int
		RULE_start = 0, RULE_statement = 1, RULE_persistentAssignment = 2, RULE_optionalExpr = 3, 
		RULE_expr = 4, RULE_exprComplex = 5, RULE_timeExpr = 6, RULE_defHierarchical = 7, 
		RULE_ruleClauseHierarchical = 8, RULE_ruleItemHierarchical = 9, RULE_hierRuleSignature = 10, 
		RULE_valueDomainSignature = 11, RULE_codeItemRelation = 12, RULE_codeItemRelationClause = 13, 
		RULE_codeItemRef = 14, RULE_defDatapoint = 15, RULE_ruleClauseDatapoint = 16, 
		RULE_ruleItemDatapoint = 17, RULE_rulesetSignature = 18, RULE_varSignature = 19, 
		RULE_defExpr = 20, RULE_defOperator = 21, RULE_parameterItem = 22, RULE_callFunction = 23, 
		RULE_exprAtom = 24, RULE_ref = 25, RULE_identifierList = 26, RULE_lists = 27, 
		RULE_evalExpr = 28, RULE_castExpr = 29, RULE_periodExpr = 30, RULE_timeShiftExpr = 31, 
		RULE_timeSeriesExpr = 32, RULE_timeAggExpr = 33, RULE_validationExpr = 34, 
		RULE_validationDatapoint = 35, RULE_validationHierarchical = 36, RULE_erCode = 37, 
		RULE_erLevel = 38, RULE_hierarchyExpr = 39, RULE_datasetClause = 40, RULE_anFunctionClause = 41, 
		RULE_partitionByClause = 42, RULE_orderByClause = 43, RULE_windowingClause = 44, 
		RULE_limitClauseItem = 45, RULE_joinExpr = 46, RULE_joinClause = 47, RULE_joinBody = 48, 
		RULE_joinCalcClause = 49, RULE_joinCalcClauseItem = 50, RULE_joinCalcExpr = 51, 
		RULE_joinAggClause = 52, RULE_joinAggClauseItem = 53, RULE_joinAggExpr = 54, 
		RULE_joinKeepClause = 55, RULE_joinDropClause = 56, RULE_joinFilterClause = 57, 
		RULE_joinRenameClause = 58, RULE_joinApplyClause = 59, RULE_anFunction = 60, 
		RULE_aggregateClause = 61, RULE_aggrFunctionClause = 62, RULE_getFiltersClause = 63, 
		RULE_getFilterClause = 64, RULE_aggrClause = 65, RULE_filterClause = 66, 
		RULE_renameClause = 67, RULE_aggrFunction = 68, RULE_calcClause = 69, 
		RULE_calcClauseItem = 70, RULE_calcExpr = 71, RULE_dropClause = 72, RULE_dropClauseItem = 73, 
		RULE_keepClause = 74, RULE_keepClauseItem = 75, RULE_unpivotExpr = 76, 
		RULE_pivotExpr = 77, RULE_subspaceExpr = 78, RULE_inBetweenClause = 79, 
		RULE_setExpr = 80, RULE_subscriptExpr = 81, RULE_aggrInvocation = 82, 
		RULE_aggrInvocationCompExpr = 83, RULE_aggrFunctionName = 84, RULE_groupingClause = 85, 
		RULE_havingClause = 86, RULE_returnAll = 87, RULE_componentRole = 88, 
		RULE_viralAttribute = 89, RULE_logBase = 90, RULE_exponent = 91, RULE_persistentDatasetID = 92, 
		RULE_datasetID = 93, RULE_rulesetID = 94, RULE_varID = 95, RULE_componentID = 96, 
		RULE_operatorID = 97, RULE_routineName = 98, RULE_joinKeyword = 99, RULE_groupKeyword = 100, 
		RULE_constant = 101, RULE_componentType2 = 102, RULE_scalarType = 103, 
		RULE_basicScalarType = 104, RULE_valueDomainName = 105, RULE_setName = 106, 
		RULE_scalarTypeConstraint = 107, RULE_dataType = 108, RULE_componentType = 109, 
		RULE_datasetType = 110, RULE_compConstraint = 111, RULE_multModifier = 112, 
		RULE_rulesetType = 113, RULE_dpRuleset = 114, RULE_hrRuleset = 115, RULE_prodValueDomains = 116, 
		RULE_prodVariables = 117, RULE_operatorType = 118, RULE_inputParameterType = 119, 
		RULE_outputParameterType = 120, RULE_scalarSetType = 121, RULE_retainType = 122, 
		RULE_defineDatapointRuleset = 123, RULE_defineHierarchicalRuleset = 124, 
		RULE_endDatapointRuleset = 125, RULE_endHierarchicalRuleset = 126, RULE_defineDataStructure = 127;
	private static String[] makeRuleNames() {
		return new String[] {
			"start", "statement", "persistentAssignment", "optionalExpr", "expr", 
			"exprComplex", "timeExpr", "defHierarchical", "ruleClauseHierarchical", 
			"ruleItemHierarchical", "hierRuleSignature", "valueDomainSignature", 
			"codeItemRelation", "codeItemRelationClause", "codeItemRef", "defDatapoint", 
			"ruleClauseDatapoint", "ruleItemDatapoint", "rulesetSignature", "varSignature", 
			"defExpr", "defOperator", "parameterItem", "callFunction", "exprAtom", 
			"ref", "identifierList", "lists", "evalExpr", "castExpr", "periodExpr", 
			"timeShiftExpr", "timeSeriesExpr", "timeAggExpr", "validationExpr", "validationDatapoint", 
			"validationHierarchical", "erCode", "erLevel", "hierarchyExpr", "datasetClause", 
			"anFunctionClause", "partitionByClause", "orderByClause", "windowingClause", 
			"limitClauseItem", "joinExpr", "joinClause", "joinBody", "joinCalcClause", 
			"joinCalcClauseItem", "joinCalcExpr", "joinAggClause", "joinAggClauseItem", 
			"joinAggExpr", "joinKeepClause", "joinDropClause", "joinFilterClause", 
			"joinRenameClause", "joinApplyClause", "anFunction", "aggregateClause", 
			"aggrFunctionClause", "getFiltersClause", "getFilterClause", "aggrClause", 
			"filterClause", "renameClause", "aggrFunction", "calcClause", "calcClauseItem", 
			"calcExpr", "dropClause", "dropClauseItem", "keepClause", "keepClauseItem", 
			"unpivotExpr", "pivotExpr", "subspaceExpr", "inBetweenClause", "setExpr", 
			"subscriptExpr", "aggrInvocation", "aggrInvocationCompExpr", "aggrFunctionName", 
			"groupingClause", "havingClause", "returnAll", "componentRole", "viralAttribute", 
			"logBase", "exponent", "persistentDatasetID", "datasetID", "rulesetID", 
			"varID", "componentID", "operatorID", "routineName", "joinKeyword", "groupKeyword", 
			"constant", "componentType2", "scalarType", "basicScalarType", "valueDomainName", 
			"setName", "scalarTypeConstraint", "dataType", "componentType", "datasetType", 
			"compConstraint", "multModifier", "rulesetType", "dpRuleset", "hrRuleset", 
			"prodValueDomains", "prodVariables", "operatorType", "inputParameterType", 
			"outputParameterType", "scalarSetType", "retainType", "defineDatapointRuleset", 
			"defineHierarchicalRuleset", "endDatapointRuleset", "endHierarchicalRuleset", 
			"defineDataStructure"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'['", "']'", "'+'", "'-'", "'*'", "'/'", "'>'", "'<'", "'<='", 
			"'>='", "'='", "'<>'", "'('", "')'", "':'", "'{'", "'}'", "'->'", "':='", 
			"'#'", "'eval'", "'if'", "'then'", "'else'", "'using'", "'with'", "'current_date'", 
			"'on'", "'drop'", "'keep'", "'calc'", "'attrcalc'", "'rename'", "'as'", 
			"'and'", "'or'", "'xor'", "'not'", "'between'", "'in'", "'not_in'", "'isnull'", 
			"'ex'", "'union'", "'diff'", "'symdiff'", "'intersect'", "'keys'", "','", 
			"'intyear'", "'intmonth'", "'intday'", "'check'", "'exists_in'", "'to'", 
			"'return'", "'imbalance'", "'errorcode'", "'all'", "'aggr'", "'errorlevel'", 
			"'order'", "'by'", "'rank'", "'asc'", "'desc'", "'min'", "'max'", "'first'", 
			"'last'", "'indexof'", "'abs'", "'key'", "'ln'", "'log'", "'trunc'", 
			"'round'", "'power'", "'mod'", "'length'", "'||'", "'trim'", "'upper'", 
			"'lower'", "'substr'", "'sum'", "'avg'", "'median'", "'count'", "'identifier'", 
			"'measure'", "'attribute'", "'filter'", "'merge'", "'exp'", "'role'", 
			"'viral'", "'match_characters'", "'type'", "'nvl'", "'hierarchy'", "'_'", 
			"'invalid'", "'valuedomain'", "'variable'", "'data'", "'structure'", 
			"'dataset'", "'operator'", "'define'", "'<-'", "'datapoint'", "'hierarchical'", 
			"'ruleset'", "'rule'", "'end'", "'alterDataset'", "'ltrim'", "'rtrim'", 
			"'instr'", "'replace'", "'ceil'", "'floor'", "'sqrt'", "'any'", "'setdiff'", 
			"'stddev_pop'", "'stddev_samp'", "'var_pop'", "'var_samp'", "'group'", 
			"'except'", "'having'", "'first_value'", "'last_value'", "'lag'", "'lead'", 
			"'ratio_to_report'", "'over'", "'preceding'", "'following'", "'unbounded'", 
			"'partition'", "'rows'", "'range'", "'current'", "'valid'", "'fill_time_series'", 
			"'flow_to_stock'", "'stock_to_flow'", "'timeshift'", "'measures'", "'no_measures'", 
			"'condition'", "'boolean'", "'date'", "'time_period'", "'number'", "'string'", 
			"'integer'", "'float'", "'list'", "'record'", "'restrict'", "'yyyy'", 
			"'mm'", "'dd'", "'maxLength'", "'regexp'", "'is'", "'when'", "'from'", 
			"'aggregates'", "'points'", "'point'", "'total'", "'partial'", "'always'", 
			"'inner_join'", "'left_join'", "'cross_join'", "'full_join'", "'maps_from'", 
			"'maps_to'", "'map_to'", "'map_from'", "'returns'", "'pivot'", "'unpivot'", 
			"'sub'", "'apply'", "'conditioned'", "'period_indicator'", "'single'", 
			"'duration'", "'time_agg'", "'unit'", "'Value'", "'valuedomains'", "'variables'", 
			"'input'", "'output'", "'cast'", "'rule_priority'", "'dataset_priority'", 
			"'default'", "'check_datapoint'", "'check_hierarchy'", "'computed'", 
			"'non_null'", "'non_zero'", "'partial_null'", "'partial_zero'", "'always_null'", 
			"'always_zero'", "'components'", "'all_measures'", "'scalar'", "'component'", 
			"'datapoint_on_valuedomains'", "'datapoint_on_variables'", "'hierarchical_on_valuedomains'", 
			"'hierarchical_on_variables'", "'set'", "'language'", null, null, null, 
			null, null, "'null'", null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, "';'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, "ASSIGN", "MEMBERSHIP", "EVAL", 
			"IF", "THEN", "ELSE", "USING", "WITH", "CURRENT_DATE", "ON", "DROP", 
			"KEEP", "CALC", "ATTRCALC", "RENAME", "AS", "AND", "OR", "XOR", "NOT", 
			"BETWEEN", "IN", "NOT_IN", "ISNULL", "EX", "UNION", "DIFF", "SYMDIFF", 
			"INTERSECT", "KEYS", "CARTESIAN_PER", "INTYEAR", "INTMONTH", "INTDAY", 
			"CHECK", "EXISTS_IN", "TO", "RETURN", "IMBALANCE", "ERRORCODE", "ALL", 
			"AGGREGATE", "ERRORLEVEL", "ORDER", "BY", "RANK", "ASC", "DESC", "MIN", 
			"MAX", "FIRST", "LAST", "INDEXOF", "ABS", "KEY", "LN", "LOG", "TRUNC", 
			"ROUND", "POWER", "MOD", "LEN", "CONCAT", "TRIM", "UCASE", "LCASE", "SUBSTR", 
			"SUM", "AVG", "MEDIAN", "COUNT", "DIMENSION", "MEASURE", "ATTRIBUTE", 
			"FILTER", "MERGE", "EXP", "ROLE", "VIRAL", "CHARSET_MATCH", "TYPE", "NVL", 
			"HIERARCHY", "OPTIONAL", "INVALID", "VALUE_DOMAIN", "VARIABLE", "DATA", 
			"STRUCTURE", "DATASET", "OPERATOR", "DEFINE", "PUT_SYMBOL", "DATAPOINT", 
			"HIERARCHICAL", "RULESET", "RULE", "END", "ALTER_DATASET", "LTRIM", "RTRIM", 
			"INSTR", "REPLACE", "CEIL", "FLOOR", "SQRT", "ANY", "SETDIFF", "STDDEV_POP", 
			"STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "GROUP", "EXCEPT", "HAVING", "FIRST_VALUE", 
			"LAST_VALUE", "LAG", "LEAD", "RATIO_TO_REPORT", "OVER", "PRECEDING", 
			"FOLLOWING", "UNBOUNDED", "PARTITION", "ROWS", "RANGE", "CURRENT", "VALID", 
			"FILL_TIME_SERIES", "FLOW_TO_STOCK", "STOCK_TO_FLOW", "TIMESHIFT", "MEASURES", 
			"NO_MEASURES", "CONDITION", "BOOLEAN", "DATE", "TIME_PERIOD", "NUMBER", 
			"STRING", "INTEGER", "FLOAT", "LIST", "RECORD", "RESTRICT", "YYYY", "MM", 
			"DD", "MAX_LENGTH", "REGEXP", "IS", "WHEN", "FROM", "AGGREGATES", "POINTS", 
			"POINT", "TOTAL", "PARTIAL", "ALWAYS", "INNER_JOIN", "LEFT_JOIN", "CROSS_JOIN", 
			"FULL_JOIN", "MAPS_FROM", "MAPS_TO", "MAP_TO", "MAP_FROM", "RETURNS", 
			"PIVOT", "UNPIVOT", "SUBSPACE", "APPLY", "CONDITIONED", "PERIOD_INDICATOR", 
			"SINGLE", "DURATION", "TIME_AGG", "UNIT", "VALUE", "VALUEDOMAINS", "VARIABLES", 
			"INPUT", "OUTPUT", "CAST", "RULE_PRIORITY", "DATASET_PRIORITY", "DEFAULT", 
			"CHECK_DATAPOINT", "CHECK_HIERARCHY", "COMPUTED", "NON_NULL", "NON_ZERO", 
			"PARTIAL_NULL", "PARTIAL_ZERO", "ALWAYS_NULL", "ALWAYS_ZERO", "COMPONENTS", 
			"ALL_MEASURES", "SCALAR", "COMPONENT", "DATAPOINT_ON_VD", "DATAPOINT_ON_VAR", 
			"HIERARCHICAL_ON_VD", "HIERARCHICAL_ON_VAR", "SET", "LANGUAGE", "INTEGER_CONSTANT", 
			"POSITIVE_CONSTANT", "NEGATIVE_CONSTANT", "FLOAT_CONSTANT", "BOOLEAN_CONSTANT", 
			"NULL_CONSTANT", "STRING_CONSTANT", "IDENTIFIER", "DIGITS0_9", "MONTH", 
			"DAY", "YEAR", "WEEK", "HOURS", "MINUTES", "SECONDS", "DATE_FORMAT", 
			"TIME_FORMAT", "TIME_UNIT", "TIME", "WS", "EOL", "ML_COMMENT", "SL_COMMENT", 
			"COMPARISON_OP", "FREQUENCY"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Vtl.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public VtlParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class StartContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(VtlParser.EOF, 0); }
		public List<TerminalNode> EOL() { return getTokens(VtlParser.EOL); }
		public TerminalNode EOL(int i) {
			return getToken(VtlParser.EOL, i);
		}
		public List<StatementContext> statement() {
			return getRuleContexts(StatementContext.class);
		}
		public StatementContext statement(int i) {
			return getRuleContext(StatementContext.class,i);
		}
		public List<TerminalNode> ML_COMMENT() { return getTokens(VtlParser.ML_COMMENT); }
		public TerminalNode ML_COMMENT(int i) {
			return getToken(VtlParser.ML_COMMENT, i);
		}
		public List<TerminalNode> SL_COMMENT() { return getTokens(VtlParser.SL_COMMENT); }
		public TerminalNode SL_COMMENT(int i) {
			return getToken(VtlParser.SL_COMMENT, i);
		}
		public StartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_start; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterStart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitStart(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitStart(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StartContext start() throws RecognitionException {
		StartContext _localctx = new StartContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_start);
		int _la;
		try {
			int _alt;
			setState(283);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(274);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(257);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << T__12) | (1L << EVAL) | (1L << IF) | (1L << CURRENT_DATE) | (1L << NOT) | (1L << BETWEEN) | (1L << ISNULL) | (1L << UNION) | (1L << SYMDIFF) | (1L << INTERSECT) | (1L << CHECK) | (1L << EXISTS_IN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (RANK - 64)) | (1L << (MIN - 64)) | (1L << (MAX - 64)) | (1L << (ABS - 64)) | (1L << (LN - 64)) | (1L << (LOG - 64)) | (1L << (TRUNC - 64)) | (1L << (ROUND - 64)) | (1L << (POWER - 64)) | (1L << (MOD - 64)) | (1L << (LEN - 64)) | (1L << (TRIM - 64)) | (1L << (UCASE - 64)) | (1L << (LCASE - 64)) | (1L << (SUBSTR - 64)) | (1L << (SUM - 64)) | (1L << (AVG - 64)) | (1L << (MEDIAN - 64)) | (1L << (COUNT - 64)) | (1L << (EXP - 64)) | (1L << (CHARSET_MATCH - 64)) | (1L << (NVL - 64)) | (1L << (HIERARCHY - 64)) | (1L << (DEFINE - 64)) | (1L << (LTRIM - 64)) | (1L << (RTRIM - 64)) | (1L << (INSTR - 64)) | (1L << (REPLACE - 64)) | (1L << (CEIL - 64)) | (1L << (FLOOR - 64)) | (1L << (SQRT - 64)) | (1L << (SETDIFF - 64)) | (1L << (STDDEV_POP - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STDDEV_SAMP - 128)) | (1L << (VAR_POP - 128)) | (1L << (VAR_SAMP - 128)) | (1L << (FIRST_VALUE - 128)) | (1L << (LAST_VALUE - 128)) | (1L << (LAG - 128)) | (1L << (LEAD - 128)) | (1L << (RATIO_TO_REPORT - 128)) | (1L << (FILL_TIME_SERIES - 128)) | (1L << (FLOW_TO_STOCK - 128)) | (1L << (STOCK_TO_FLOW - 128)) | (1L << (TIMESHIFT - 128)) | (1L << (INNER_JOIN - 128)) | (1L << (LEFT_JOIN - 128)) | (1L << (CROSS_JOIN - 128)) | (1L << (FULL_JOIN - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PERIOD_INDICATOR - 193)) | (1L << (TIME_AGG - 193)) | (1L << (CAST - 193)) | (1L << (CHECK_DATAPOINT - 193)) | (1L << (CHECK_HIERARCHY - 193)) | (1L << (INTEGER_CONSTANT - 193)) | (1L << (FLOAT_CONSTANT - 193)) | (1L << (BOOLEAN_CONSTANT - 193)) | (1L << (NULL_CONSTANT - 193)) | (1L << (STRING_CONSTANT - 193)) | (1L << (IDENTIFIER - 193)))) != 0)) {
							{
							setState(256);
							statement();
							}
						}

						setState(262);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==ML_COMMENT) {
							{
							{
							setState(259);
							match(ML_COMMENT);
							}
							}
							setState(264);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(268);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==SL_COMMENT) {
							{
							{
							setState(265);
							match(SL_COMMENT);
							}
							}
							setState(270);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(271);
						match(EOL);
						}
						} 
					}
					setState(276);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
				}
				setState(278);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << T__12) | (1L << EVAL) | (1L << IF) | (1L << CURRENT_DATE) | (1L << NOT) | (1L << BETWEEN) | (1L << ISNULL) | (1L << UNION) | (1L << SYMDIFF) | (1L << INTERSECT) | (1L << CHECK) | (1L << EXISTS_IN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (RANK - 64)) | (1L << (MIN - 64)) | (1L << (MAX - 64)) | (1L << (ABS - 64)) | (1L << (LN - 64)) | (1L << (LOG - 64)) | (1L << (TRUNC - 64)) | (1L << (ROUND - 64)) | (1L << (POWER - 64)) | (1L << (MOD - 64)) | (1L << (LEN - 64)) | (1L << (TRIM - 64)) | (1L << (UCASE - 64)) | (1L << (LCASE - 64)) | (1L << (SUBSTR - 64)) | (1L << (SUM - 64)) | (1L << (AVG - 64)) | (1L << (MEDIAN - 64)) | (1L << (COUNT - 64)) | (1L << (EXP - 64)) | (1L << (CHARSET_MATCH - 64)) | (1L << (NVL - 64)) | (1L << (HIERARCHY - 64)) | (1L << (DEFINE - 64)) | (1L << (LTRIM - 64)) | (1L << (RTRIM - 64)) | (1L << (INSTR - 64)) | (1L << (REPLACE - 64)) | (1L << (CEIL - 64)) | (1L << (FLOOR - 64)) | (1L << (SQRT - 64)) | (1L << (SETDIFF - 64)) | (1L << (STDDEV_POP - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STDDEV_SAMP - 128)) | (1L << (VAR_POP - 128)) | (1L << (VAR_SAMP - 128)) | (1L << (FIRST_VALUE - 128)) | (1L << (LAST_VALUE - 128)) | (1L << (LAG - 128)) | (1L << (LEAD - 128)) | (1L << (RATIO_TO_REPORT - 128)) | (1L << (FILL_TIME_SERIES - 128)) | (1L << (FLOW_TO_STOCK - 128)) | (1L << (STOCK_TO_FLOW - 128)) | (1L << (TIMESHIFT - 128)) | (1L << (INNER_JOIN - 128)) | (1L << (LEFT_JOIN - 128)) | (1L << (CROSS_JOIN - 128)) | (1L << (FULL_JOIN - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PERIOD_INDICATOR - 193)) | (1L << (TIME_AGG - 193)) | (1L << (CAST - 193)) | (1L << (CHECK_DATAPOINT - 193)) | (1L << (CHECK_HIERARCHY - 193)) | (1L << (INTEGER_CONSTANT - 193)) | (1L << (FLOAT_CONSTANT - 193)) | (1L << (BOOLEAN_CONSTANT - 193)) | (1L << (NULL_CONSTANT - 193)) | (1L << (STRING_CONSTANT - 193)) | (1L << (IDENTIFIER - 193)))) != 0)) {
					{
					setState(277);
					statement();
					}
				}

				setState(280);
				match(EOF);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(281);
				match(ML_COMMENT);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(282);
				match(SL_COMMENT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(VtlParser.ASSIGN, 0); }
		public PersistentAssignmentContext persistentAssignment() {
			return getRuleContext(PersistentAssignmentContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		try {
			setState(292);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(288);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(285);
					varID();
					setState(286);
					match(ASSIGN);
					}
					break;
				}
				setState(290);
				expr(0);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(291);
				persistentAssignment();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PersistentAssignmentContext extends ParserRuleContext {
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public TerminalNode PUT_SYMBOL() { return getToken(VtlParser.PUT_SYMBOL, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public PersistentAssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_persistentAssignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPersistentAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPersistentAssignment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPersistentAssignment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PersistentAssignmentContext persistentAssignment() throws RecognitionException {
		PersistentAssignmentContext _localctx = new PersistentAssignmentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_persistentAssignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(294);
			varID();
			setState(295);
			match(PUT_SYMBOL);
			setState(296);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OptionalExprContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode OPTIONAL() { return getToken(VtlParser.OPTIONAL, 0); }
		public OptionalExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_optionalExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterOptionalExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitOptionalExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitOptionalExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OptionalExprContext optionalExpr() throws RecognitionException {
		OptionalExprContext _localctx = new OptionalExprContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_optionalExpr);
		try {
			setState(300);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__2:
			case T__3:
			case T__12:
			case EVAL:
			case IF:
			case CURRENT_DATE:
			case NOT:
			case BETWEEN:
			case ISNULL:
			case UNION:
			case SYMDIFF:
			case INTERSECT:
			case CHECK:
			case EXISTS_IN:
			case RANK:
			case MIN:
			case MAX:
			case ABS:
			case LN:
			case LOG:
			case TRUNC:
			case ROUND:
			case POWER:
			case MOD:
			case LEN:
			case TRIM:
			case UCASE:
			case LCASE:
			case SUBSTR:
			case SUM:
			case AVG:
			case MEDIAN:
			case COUNT:
			case EXP:
			case CHARSET_MATCH:
			case NVL:
			case HIERARCHY:
			case DEFINE:
			case LTRIM:
			case RTRIM:
			case INSTR:
			case REPLACE:
			case CEIL:
			case FLOOR:
			case SQRT:
			case SETDIFF:
			case STDDEV_POP:
			case STDDEV_SAMP:
			case VAR_POP:
			case VAR_SAMP:
			case FIRST_VALUE:
			case LAST_VALUE:
			case LAG:
			case LEAD:
			case RATIO_TO_REPORT:
			case FILL_TIME_SERIES:
			case FLOW_TO_STOCK:
			case STOCK_TO_FLOW:
			case TIMESHIFT:
			case INNER_JOIN:
			case LEFT_JOIN:
			case CROSS_JOIN:
			case FULL_JOIN:
			case PERIOD_INDICATOR:
			case TIME_AGG:
			case CAST:
			case CHECK_DATAPOINT:
			case CHECK_HIERARCHY:
			case INTEGER_CONSTANT:
			case FLOAT_CONSTANT:
			case BOOLEAN_CONSTANT:
			case NULL_CONSTANT:
			case STRING_CONSTANT:
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(298);
				expr(0);
				}
				break;
			case OPTIONAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(299);
				match(OPTIONAL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public ExprAtomContext exprAtom() {
			return getRuleContext(ExprAtomContext.class,0);
		}
		public TerminalNode MEMBERSHIP() { return getToken(VtlParser.MEMBERSHIP, 0); }
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public List<DatasetClauseContext> datasetClause() {
			return getRuleContexts(DatasetClauseContext.class);
		}
		public DatasetClauseContext datasetClause(int i) {
			return getRuleContext(DatasetClauseContext.class,i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> ASSIGN() { return getTokens(VtlParser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(VtlParser.ASSIGN, i);
		}
		public TerminalNode NOT() { return getToken(VtlParser.NOT, 0); }
		public TerminalNode EXISTS_IN() { return getToken(VtlParser.EXISTS_IN, 0); }
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public RetainTypeContext retainType() {
			return getRuleContext(RetainTypeContext.class,0);
		}
		public TerminalNode IF() { return getToken(VtlParser.IF, 0); }
		public TerminalNode THEN() { return getToken(VtlParser.THEN, 0); }
		public TerminalNode ELSE() { return getToken(VtlParser.ELSE, 0); }
		public ExprComplexContext exprComplex() {
			return getRuleContext(ExprComplexContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public TerminalNode AND() { return getToken(VtlParser.AND, 0); }
		public TerminalNode OR() { return getToken(VtlParser.OR, 0); }
		public TerminalNode XOR() { return getToken(VtlParser.XOR, 0); }
		public TerminalNode CONCAT() { return getToken(VtlParser.CONCAT, 0); }
		public TerminalNode IN() { return getToken(VtlParser.IN, 0); }
		public TerminalNode NOT_IN() { return getToken(VtlParser.NOT_IN, 0); }
		public ListsContext lists() {
			return getRuleContext(ListsContext.class,0);
		}
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(303);
				exprAtom();
				setState(316);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(304);
						match(T__0);
						setState(310);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case DROP:
						case KEEP:
						case CALC:
						case RENAME:
						case AGGREGATE:
						case FILTER:
						case PIVOT:
						case UNPIVOT:
						case SUBSPACE:
							{
							setState(305);
							datasetClause();
							}
							break;
						case T__2:
						case T__3:
						case T__12:
						case EVAL:
						case IF:
						case CURRENT_DATE:
						case NOT:
						case BETWEEN:
						case ISNULL:
						case UNION:
						case SYMDIFF:
						case INTERSECT:
						case CHECK:
						case EXISTS_IN:
						case RANK:
						case MIN:
						case MAX:
						case ABS:
						case LN:
						case LOG:
						case TRUNC:
						case ROUND:
						case POWER:
						case MOD:
						case LEN:
						case TRIM:
						case UCASE:
						case LCASE:
						case SUBSTR:
						case SUM:
						case AVG:
						case MEDIAN:
						case COUNT:
						case EXP:
						case CHARSET_MATCH:
						case NVL:
						case HIERARCHY:
						case DEFINE:
						case LTRIM:
						case RTRIM:
						case INSTR:
						case REPLACE:
						case CEIL:
						case FLOOR:
						case SQRT:
						case SETDIFF:
						case STDDEV_POP:
						case STDDEV_SAMP:
						case VAR_POP:
						case VAR_SAMP:
						case FIRST_VALUE:
						case LAST_VALUE:
						case LAG:
						case LEAD:
						case RATIO_TO_REPORT:
						case FILL_TIME_SERIES:
						case FLOW_TO_STOCK:
						case STOCK_TO_FLOW:
						case TIMESHIFT:
						case INNER_JOIN:
						case LEFT_JOIN:
						case CROSS_JOIN:
						case FULL_JOIN:
						case PERIOD_INDICATOR:
						case TIME_AGG:
						case CAST:
						case CHECK_DATAPOINT:
						case CHECK_HIERARCHY:
						case INTEGER_CONSTANT:
						case FLOAT_CONSTANT:
						case BOOLEAN_CONSTANT:
						case NULL_CONSTANT:
						case STRING_CONSTANT:
						case IDENTIFIER:
							{
							{
							setState(306);
							expr(0);
							setState(307);
							match(ASSIGN);
							setState(308);
							expr(0);
							}
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						setState(312);
						match(T__1);
						}
						} 
					}
					setState(318);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
				}
				setState(321);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(319);
					match(MEMBERSHIP);
					setState(320);
					componentID();
					}
					break;
				}
				}
				break;
			case 2:
				{
				setState(323);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << NOT))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(324);
				expr(16);
				}
				break;
			case 3:
				{
				setState(325);
				match(EXISTS_IN);
				setState(326);
				match(T__12);
				setState(327);
				expr(0);
				setState(328);
				match(CARTESIAN_PER);
				setState(329);
				expr(0);
				setState(332);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(330);
					match(CARTESIAN_PER);
					setState(331);
					retainType();
					}
				}

				setState(334);
				match(T__13);
				}
				break;
			case 4:
				{
				setState(336);
				match(IF);
				setState(337);
				expr(0);
				setState(338);
				match(THEN);
				setState(339);
				expr(0);
				setState(340);
				match(ELSE);
				setState(341);
				expr(6);
				}
				break;
			case 5:
				{
				setState(343);
				exprComplex();
				}
				break;
			case 6:
				{
				setState(344);
				exprAtom();
				}
				break;
			case 7:
				{
				setState(345);
				constant();
				}
				break;
			case 8:
				{
				setState(346);
				match(IDENTIFIER);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(384);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(382);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
					case 1:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(349);
						if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
						setState(350);
						_la = _input.LA(1);
						if ( !(_la==T__4 || _la==T__5) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(351);
						expr(16);
						}
						break;
					case 2:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(352);
						if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
						setState(353);
						_la = _input.LA(1);
						if ( !(_la==T__2 || _la==T__3) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(354);
						expr(15);
						}
						break;
					case 3:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(355);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(356);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(357);
						expr(14);
						}
						break;
					case 4:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(358);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(359);
						_la = _input.LA(1);
						if ( !(_la==T__10 || _la==T__11) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(360);
						expr(10);
						}
						break;
					case 5:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(361);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(362);
						match(AND);
						setState(363);
						expr(9);
						}
						break;
					case 6:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(364);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(365);
						_la = _input.LA(1);
						if ( !(_la==OR || _la==XOR) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(366);
						expr(8);
						}
						break;
					case 7:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(367);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(368);
						match(CONCAT);
						setState(369);
						expr(4);
						}
						break;
					case 8:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(370);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(371);
						_la = _input.LA(1);
						if ( !(_la==IN || _la==NOT_IN) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(374);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case T__15:
							{
							setState(372);
							lists();
							}
							break;
						case IDENTIFIER:
							{
							setState(373);
							match(IDENTIFIER);
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						break;
					case 9:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(376);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(377);
						match(EXISTS_IN);
						setState(378);
						expr(0);
						setState(380);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
						case 1:
							{
							setState(379);
							match(ALL);
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(386);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ExprComplexContext extends ParserRuleContext {
		public ExprComplexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exprComplex; }
	 
		public ExprComplexContext() { }
		public void copyFrom(ExprComplexContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SimpleaggregateFunctionsContext extends ExprComplexContext {
		public AggrFunctionContext aggrFunction() {
			return getRuleContext(AggrFunctionContext.class,0);
		}
		public SimpleaggregateFunctionsContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSimpleaggregateFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSimpleaggregateFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSimpleaggregateFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StandaloneAggregateFunctionContext extends ExprComplexContext {
		public AggrInvocationContext aggrInvocation() {
			return getRuleContext(AggrInvocationContext.class,0);
		}
		public StandaloneAggregateFunctionContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterStandaloneAggregateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitStandaloneAggregateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitStandaloneAggregateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DefinitionExpressionsContext extends ExprComplexContext {
		public DefExprContext defExpr() {
			return getRuleContext(DefExprContext.class,0);
		}
		public DefinitionExpressionsContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefinitionExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefinitionExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefinitionExpressions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TimeexpressionsContext extends ExprComplexContext {
		public TimeExprContext timeExpr() {
			return getRuleContext(TimeExprContext.class,0);
		}
		public TimeexpressionsContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTimeexpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTimeexpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTimeexpressions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class JoinExpressionContext extends ExprComplexContext {
		public JoinExprContext joinExpr() {
			return getRuleContext(JoinExprContext.class,0);
		}
		public JoinExpressionContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CallFunctionExpressionContext extends ExprComplexContext {
		public CallFunctionContext callFunction() {
			return getRuleContext(CallFunctionContext.class,0);
		}
		public CallFunctionExpressionContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCallFunctionExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCallFunctionExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCallFunctionExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExprValidationExprContext extends ExprComplexContext {
		public ValidationExprContext validationExpr() {
			return getRuleContext(ValidationExprContext.class,0);
		}
		public ExprValidationExprContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterExprValidationExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitExprValidationExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitExprValidationExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StandaloneAnalyticFunctionContext extends ExprComplexContext {
		public AnFunctionClauseContext anFunctionClause() {
			return getRuleContext(AnFunctionClauseContext.class,0);
		}
		public StandaloneAnalyticFunctionContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterStandaloneAnalyticFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitStandaloneAnalyticFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitStandaloneAnalyticFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComponentExpressionwithAggrClauseContext extends ExprComplexContext {
		public AggrInvocationCompExprContext aggrInvocationCompExpr() {
			return getRuleContext(AggrInvocationCompExprContext.class,0);
		}
		public ComponentExpressionwithAggrClauseContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterComponentExpressionwithAggrClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitComponentExpressionwithAggrClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitComponentExpressionwithAggrClause(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetExpressionsContext extends ExprComplexContext {
		public SetExprContext setExpr() {
			return getRuleContext(SetExprContext.class,0);
		}
		public SetExpressionsContext(ExprComplexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSetExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSetExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSetExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprComplexContext exprComplex() throws RecognitionException {
		ExprComplexContext _localctx = new ExprComplexContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_exprComplex);
		try {
			setState(397);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
			case 1:
				_localctx = new ExprValidationExprContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(387);
				validationExpr();
				}
				break;
			case 2:
				_localctx = new DefinitionExpressionsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(388);
				defExpr();
				}
				break;
			case 3:
				_localctx = new StandaloneAggregateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(389);
				aggrInvocation();
				}
				break;
			case 4:
				_localctx = new ComponentExpressionwithAggrClauseContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(390);
				aggrInvocationCompExpr();
				}
				break;
			case 5:
				_localctx = new StandaloneAnalyticFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(391);
				anFunctionClause();
				}
				break;
			case 6:
				_localctx = new SimpleaggregateFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(392);
				aggrFunction();
				}
				break;
			case 7:
				_localctx = new TimeexpressionsContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(393);
				timeExpr();
				}
				break;
			case 8:
				_localctx = new SetExpressionsContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(394);
				setExpr();
				}
				break;
			case 9:
				_localctx = new CallFunctionExpressionContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(395);
				callFunction();
				}
				break;
			case 10:
				_localctx = new JoinExpressionContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(396);
				joinExpr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TimeExprContext extends ParserRuleContext {
		public Token opComp;
		public TimeSeriesExprContext timeSeriesExpr() {
			return getRuleContext(TimeSeriesExprContext.class,0);
		}
		public PeriodExprContext periodExpr() {
			return getRuleContext(PeriodExprContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TimeShiftExprContext timeShiftExpr() {
			return getRuleContext(TimeShiftExprContext.class,0);
		}
		public TimeAggExprContext timeAggExpr() {
			return getRuleContext(TimeAggExprContext.class,0);
		}
		public TerminalNode CURRENT_DATE() { return getToken(VtlParser.CURRENT_DATE, 0); }
		public TimeExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTimeExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTimeExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTimeExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeExprContext timeExpr() throws RecognitionException {
		TimeExprContext _localctx = new TimeExprContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_timeExpr);
		int _la;
		try {
			setState(408);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FILL_TIME_SERIES:
				enterOuterAlt(_localctx, 1);
				{
				setState(399);
				timeSeriesExpr();
				}
				break;
			case PERIOD_INDICATOR:
				enterOuterAlt(_localctx, 2);
				{
				setState(400);
				periodExpr();
				setState(403);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(401);
					((TimeExprContext)_localctx).opComp = _input.LT(1);
					_la = _input.LA(1);
					if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11))) != 0)) ) {
						((TimeExprContext)_localctx).opComp = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(402);
					expr(0);
					}
					break;
				}
				}
				break;
			case TIMESHIFT:
				enterOuterAlt(_localctx, 3);
				{
				setState(405);
				timeShiftExpr();
				}
				break;
			case TIME_AGG:
				enterOuterAlt(_localctx, 4);
				{
				setState(406);
				timeAggExpr();
				}
				break;
			case CURRENT_DATE:
				enterOuterAlt(_localctx, 5);
				{
				setState(407);
				match(CURRENT_DATE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefHierarchicalContext extends ParserRuleContext {
		public DefineHierarchicalRulesetContext defineHierarchicalRuleset() {
			return getRuleContext(DefineHierarchicalRulesetContext.class,0);
		}
		public RulesetIDContext rulesetID() {
			return getRuleContext(RulesetIDContext.class,0);
		}
		public HierRuleSignatureContext hierRuleSignature() {
			return getRuleContext(HierRuleSignatureContext.class,0);
		}
		public TerminalNode IS() { return getToken(VtlParser.IS, 0); }
		public RuleClauseHierarchicalContext ruleClauseHierarchical() {
			return getRuleContext(RuleClauseHierarchicalContext.class,0);
		}
		public EndHierarchicalRulesetContext endHierarchicalRuleset() {
			return getRuleContext(EndHierarchicalRulesetContext.class,0);
		}
		public DefHierarchicalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defHierarchical; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefHierarchical(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefHierarchical(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefHierarchical(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefHierarchicalContext defHierarchical() throws RecognitionException {
		DefHierarchicalContext _localctx = new DefHierarchicalContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_defHierarchical);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(410);
			defineHierarchicalRuleset();
			setState(411);
			rulesetID();
			setState(412);
			match(T__12);
			setState(413);
			hierRuleSignature();
			setState(414);
			match(T__13);
			setState(415);
			match(IS);
			setState(416);
			ruleClauseHierarchical();
			setState(417);
			endHierarchicalRuleset();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleClauseHierarchicalContext extends ParserRuleContext {
		public List<RuleItemHierarchicalContext> ruleItemHierarchical() {
			return getRuleContexts(RuleItemHierarchicalContext.class);
		}
		public RuleItemHierarchicalContext ruleItemHierarchical(int i) {
			return getRuleContext(RuleItemHierarchicalContext.class,i);
		}
		public List<TerminalNode> EOL() { return getTokens(VtlParser.EOL); }
		public TerminalNode EOL(int i) {
			return getToken(VtlParser.EOL, i);
		}
		public RuleClauseHierarchicalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleClauseHierarchical; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRuleClauseHierarchical(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRuleClauseHierarchical(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRuleClauseHierarchical(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleClauseHierarchicalContext ruleClauseHierarchical() throws RecognitionException {
		RuleClauseHierarchicalContext _localctx = new RuleClauseHierarchicalContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_ruleClauseHierarchical);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(419);
			ruleItemHierarchical();
			setState(424);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==EOL) {
				{
				{
				setState(420);
				match(EOL);
				setState(421);
				ruleItemHierarchical();
				}
				}
				setState(426);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleItemHierarchicalContext extends ParserRuleContext {
		public CodeItemRelationContext codeItemRelation() {
			return getRuleContext(CodeItemRelationContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public ErCodeContext erCode() {
			return getRuleContext(ErCodeContext.class,0);
		}
		public ErLevelContext erLevel() {
			return getRuleContext(ErLevelContext.class,0);
		}
		public RuleItemHierarchicalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleItemHierarchical; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRuleItemHierarchical(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRuleItemHierarchical(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRuleItemHierarchical(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleItemHierarchicalContext ruleItemHierarchical() throws RecognitionException {
		RuleItemHierarchicalContext _localctx = new RuleItemHierarchicalContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_ruleItemHierarchical);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(429);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
			case 1:
				{
				setState(427);
				match(IDENTIFIER);
				setState(428);
				match(T__14);
				}
				break;
			}
			setState(431);
			codeItemRelation();
			setState(433);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORCODE) {
				{
				setState(432);
				erCode();
				}
			}

			setState(436);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORLEVEL) {
				{
				setState(435);
				erLevel();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HierRuleSignatureContext extends ParserRuleContext {
		public TerminalNode RULE() { return getToken(VtlParser.RULE, 0); }
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public TerminalNode VALUE_DOMAIN() { return getToken(VtlParser.VALUE_DOMAIN, 0); }
		public TerminalNode VARIABLE() { return getToken(VtlParser.VARIABLE, 0); }
		public ValueDomainSignatureContext valueDomainSignature() {
			return getRuleContext(ValueDomainSignatureContext.class,0);
		}
		public HierRuleSignatureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hierRuleSignature; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterHierRuleSignature(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitHierRuleSignature(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitHierRuleSignature(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HierRuleSignatureContext hierRuleSignature() throws RecognitionException {
		HierRuleSignatureContext _localctx = new HierRuleSignatureContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_hierRuleSignature);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(438);
			_la = _input.LA(1);
			if ( !(_la==VALUE_DOMAIN || _la==VARIABLE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(440);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CONDITION) {
				{
				setState(439);
				valueDomainSignature();
				}
			}

			setState(442);
			match(RULE);
			setState(443);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueDomainSignatureContext extends ParserRuleContext {
		public TerminalNode CONDITION() { return getToken(VtlParser.CONDITION, 0); }
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> AS() { return getTokens(VtlParser.AS); }
		public TerminalNode AS(int i) {
			return getToken(VtlParser.AS, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public ValueDomainSignatureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueDomainSignature; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValueDomainSignature(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValueDomainSignature(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValueDomainSignature(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueDomainSignatureContext valueDomainSignature() throws RecognitionException {
		ValueDomainSignatureContext _localctx = new ValueDomainSignatureContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_valueDomainSignature);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(445);
			match(CONDITION);
			setState(446);
			match(IDENTIFIER);
			setState(449);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(447);
				match(AS);
				setState(448);
				match(IDENTIFIER);
				}
			}

			setState(459);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(451);
				match(CARTESIAN_PER);
				setState(452);
				match(IDENTIFIER);
				setState(455);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(453);
					match(AS);
					setState(454);
					match(IDENTIFIER);
					}
				}

				}
				}
				setState(461);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CodeItemRelationContext extends ParserRuleContext {
		public CodeItemRefContext codeItemRef() {
			return getRuleContext(CodeItemRefContext.class,0);
		}
		public List<CodeItemRelationClauseContext> codeItemRelationClause() {
			return getRuleContexts(CodeItemRelationClauseContext.class);
		}
		public CodeItemRelationClauseContext codeItemRelationClause(int i) {
			return getRuleContext(CodeItemRelationClauseContext.class,i);
		}
		public TerminalNode WHEN() { return getToken(VtlParser.WHEN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode THEN() { return getToken(VtlParser.THEN, 0); }
		public CodeItemRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_codeItemRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCodeItemRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCodeItemRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCodeItemRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CodeItemRelationContext codeItemRelation() throws RecognitionException {
		CodeItemRelationContext _localctx = new CodeItemRelationContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_codeItemRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(466);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHEN) {
				{
				setState(462);
				match(WHEN);
				setState(463);
				expr(0);
				setState(464);
				match(THEN);
				}
			}

			setState(468);
			codeItemRef();
			setState(469);
			codeItemRelationClause();
			setState(473);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2 || _la==T__3 || _la==IDENTIFIER) {
				{
				{
				setState(470);
				codeItemRelationClause();
				}
				}
				setState(475);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CodeItemRelationClauseContext extends ParserRuleContext {
		public Token opAdd;
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public CodeItemRelationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_codeItemRelationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCodeItemRelationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCodeItemRelationClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCodeItemRelationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CodeItemRelationClauseContext codeItemRelationClause() throws RecognitionException {
		CodeItemRelationClauseContext _localctx = new CodeItemRelationClauseContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_codeItemRelationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(477);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__2 || _la==T__3) {
				{
				setState(476);
				((CodeItemRelationClauseContext)_localctx).opAdd = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==T__2 || _la==T__3) ) {
					((CodeItemRelationClauseContext)_localctx).opAdd = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(479);
			match(IDENTIFIER);
			setState(484);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(480);
				match(T__0);
				setState(481);
				expr(0);
				setState(482);
				match(T__1);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CodeItemRefContext extends ParserRuleContext {
		public Token opComp;
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public CodeItemRefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_codeItemRef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCodeItemRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCodeItemRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCodeItemRef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CodeItemRefContext codeItemRef() throws RecognitionException {
		CodeItemRefContext _localctx = new CodeItemRefContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_codeItemRef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(486);
			match(IDENTIFIER);
			setState(488);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10))) != 0)) {
				{
				setState(487);
				((CodeItemRefContext)_localctx).opComp = _input.LT(1);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10))) != 0)) ) {
					((CodeItemRefContext)_localctx).opComp = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefDatapointContext extends ParserRuleContext {
		public DefineDatapointRulesetContext defineDatapointRuleset() {
			return getRuleContext(DefineDatapointRulesetContext.class,0);
		}
		public RulesetIDContext rulesetID() {
			return getRuleContext(RulesetIDContext.class,0);
		}
		public RulesetSignatureContext rulesetSignature() {
			return getRuleContext(RulesetSignatureContext.class,0);
		}
		public TerminalNode IS() { return getToken(VtlParser.IS, 0); }
		public RuleClauseDatapointContext ruleClauseDatapoint() {
			return getRuleContext(RuleClauseDatapointContext.class,0);
		}
		public EndDatapointRulesetContext endDatapointRuleset() {
			return getRuleContext(EndDatapointRulesetContext.class,0);
		}
		public DefDatapointContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defDatapoint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefDatapoint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefDatapoint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefDatapoint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefDatapointContext defDatapoint() throws RecognitionException {
		DefDatapointContext _localctx = new DefDatapointContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_defDatapoint);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(490);
			defineDatapointRuleset();
			setState(491);
			rulesetID();
			setState(492);
			match(T__12);
			setState(493);
			rulesetSignature();
			setState(494);
			match(T__13);
			setState(495);
			match(IS);
			setState(496);
			ruleClauseDatapoint();
			setState(497);
			endDatapointRuleset();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleClauseDatapointContext extends ParserRuleContext {
		public List<RuleItemDatapointContext> ruleItemDatapoint() {
			return getRuleContexts(RuleItemDatapointContext.class);
		}
		public RuleItemDatapointContext ruleItemDatapoint(int i) {
			return getRuleContext(RuleItemDatapointContext.class,i);
		}
		public List<TerminalNode> EOL() { return getTokens(VtlParser.EOL); }
		public TerminalNode EOL(int i) {
			return getToken(VtlParser.EOL, i);
		}
		public RuleClauseDatapointContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleClauseDatapoint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRuleClauseDatapoint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRuleClauseDatapoint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRuleClauseDatapoint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleClauseDatapointContext ruleClauseDatapoint() throws RecognitionException {
		RuleClauseDatapointContext _localctx = new RuleClauseDatapointContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_ruleClauseDatapoint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(499);
			ruleItemDatapoint();
			setState(504);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==EOL) {
				{
				{
				setState(500);
				match(EOL);
				setState(501);
				ruleItemDatapoint();
				}
				}
				setState(506);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleItemDatapointContext extends ParserRuleContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public TerminalNode WHEN() { return getToken(VtlParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(VtlParser.THEN, 0); }
		public ErCodeContext erCode() {
			return getRuleContext(ErCodeContext.class,0);
		}
		public ErLevelContext erLevel() {
			return getRuleContext(ErLevelContext.class,0);
		}
		public RuleItemDatapointContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleItemDatapoint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRuleItemDatapoint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRuleItemDatapoint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRuleItemDatapoint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleItemDatapointContext ruleItemDatapoint() throws RecognitionException {
		RuleItemDatapointContext _localctx = new RuleItemDatapointContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_ruleItemDatapoint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(509);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
			case 1:
				{
				setState(507);
				match(IDENTIFIER);
				setState(508);
				match(T__14);
				}
				break;
			}
			setState(515);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHEN) {
				{
				setState(511);
				match(WHEN);
				setState(512);
				expr(0);
				setState(513);
				match(THEN);
				}
			}

			setState(517);
			expr(0);
			setState(519);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORCODE) {
				{
				setState(518);
				erCode();
				}
			}

			setState(522);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORLEVEL) {
				{
				setState(521);
				erLevel();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RulesetSignatureContext extends ParserRuleContext {
		public List<VarSignatureContext> varSignature() {
			return getRuleContexts(VarSignatureContext.class);
		}
		public VarSignatureContext varSignature(int i) {
			return getRuleContext(VarSignatureContext.class,i);
		}
		public TerminalNode VALUE_DOMAIN() { return getToken(VtlParser.VALUE_DOMAIN, 0); }
		public TerminalNode VARIABLE() { return getToken(VtlParser.VARIABLE, 0); }
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public RulesetSignatureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rulesetSignature; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRulesetSignature(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRulesetSignature(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRulesetSignature(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RulesetSignatureContext rulesetSignature() throws RecognitionException {
		RulesetSignatureContext _localctx = new RulesetSignatureContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_rulesetSignature);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(524);
			_la = _input.LA(1);
			if ( !(_la==VALUE_DOMAIN || _la==VARIABLE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(525);
			varSignature();
			setState(530);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(526);
				match(CARTESIAN_PER);
				setState(527);
				varSignature();
				}
				}
				setState(532);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VarSignatureContext extends ParserRuleContext {
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public TerminalNode AS() { return getToken(VtlParser.AS, 0); }
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public VarSignatureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varSignature; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterVarSignature(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitVarSignature(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitVarSignature(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VarSignatureContext varSignature() throws RecognitionException {
		VarSignatureContext _localctx = new VarSignatureContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_varSignature);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(533);
			varID();
			setState(536);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(534);
				match(AS);
				setState(535);
				match(IDENTIFIER);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefExprContext extends ParserRuleContext {
		public DefOperatorContext defOperator() {
			return getRuleContext(DefOperatorContext.class,0);
		}
		public DefDatapointContext defDatapoint() {
			return getRuleContext(DefDatapointContext.class,0);
		}
		public DefHierarchicalContext defHierarchical() {
			return getRuleContext(DefHierarchicalContext.class,0);
		}
		public DefExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefExprContext defExpr() throws RecognitionException {
		DefExprContext _localctx = new DefExprContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_defExpr);
		try {
			setState(541);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(538);
				defOperator();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(539);
				defDatapoint();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(540);
				defHierarchical();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefOperatorContext extends ParserRuleContext {
		public TerminalNode DEFINE() { return getToken(VtlParser.DEFINE, 0); }
		public List<TerminalNode> OPERATOR() { return getTokens(VtlParser.OPERATOR); }
		public TerminalNode OPERATOR(int i) {
			return getToken(VtlParser.OPERATOR, i);
		}
		public OperatorIDContext operatorID() {
			return getRuleContext(OperatorIDContext.class,0);
		}
		public TerminalNode IS() { return getToken(VtlParser.IS, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode END() { return getToken(VtlParser.END, 0); }
		public List<ParameterItemContext> parameterItem() {
			return getRuleContexts(ParameterItemContext.class);
		}
		public ParameterItemContext parameterItem(int i) {
			return getRuleContext(ParameterItemContext.class,i);
		}
		public TerminalNode RETURNS() { return getToken(VtlParser.RETURNS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public DefOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefOperatorContext defOperator() throws RecognitionException {
		DefOperatorContext _localctx = new DefOperatorContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_defOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(543);
			match(DEFINE);
			setState(544);
			match(OPERATOR);
			setState(545);
			operatorID();
			setState(546);
			match(T__12);
			setState(555);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IDENTIFIER) {
				{
				setState(547);
				parameterItem();
				setState(552);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(548);
					match(CARTESIAN_PER);
					setState(549);
					parameterItem();
					}
					}
					setState(554);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(557);
			match(T__13);
			setState(560);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETURNS) {
				{
				setState(558);
				match(RETURNS);
				setState(559);
				dataType();
				}
			}

			setState(562);
			match(IS);
			setState(563);
			expr(0);
			setState(564);
			match(END);
			setState(565);
			match(OPERATOR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParameterItemContext extends ParserRuleContext {
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode DEFAULT() { return getToken(VtlParser.DEFAULT, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ParameterItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameterItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterParameterItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitParameterItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitParameterItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParameterItemContext parameterItem() throws RecognitionException {
		ParameterItemContext _localctx = new ParameterItemContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_parameterItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(567);
			varID();
			setState(568);
			dataType();
			setState(571);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT) {
				{
				setState(569);
				match(DEFAULT);
				setState(570);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CallFunctionContext extends ParserRuleContext {
		public OperatorIDContext operatorID() {
			return getRuleContext(OperatorIDContext.class,0);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public List<TerminalNode> OPTIONAL() { return getTokens(VtlParser.OPTIONAL); }
		public TerminalNode OPTIONAL(int i) {
			return getToken(VtlParser.OPTIONAL, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public CallFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callFunction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCallFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCallFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCallFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallFunctionContext callFunction() throws RecognitionException {
		CallFunctionContext _localctx = new CallFunctionContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_callFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(573);
			operatorID();
			setState(574);
			match(T__12);
			setState(589);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPTIONAL || ((((_la - 226)) & ~0x3f) == 0 && ((1L << (_la - 226)) & ((1L << (INTEGER_CONSTANT - 226)) | (1L << (FLOAT_CONSTANT - 226)) | (1L << (BOOLEAN_CONSTANT - 226)) | (1L << (NULL_CONSTANT - 226)) | (1L << (STRING_CONSTANT - 226)))) != 0)) {
				{
				setState(577);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case INTEGER_CONSTANT:
				case FLOAT_CONSTANT:
				case BOOLEAN_CONSTANT:
				case NULL_CONSTANT:
				case STRING_CONSTANT:
					{
					setState(575);
					constant();
					}
					break;
				case OPTIONAL:
					{
					setState(576);
					match(OPTIONAL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(586);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(579);
					match(CARTESIAN_PER);
					setState(582);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case INTEGER_CONSTANT:
					case FLOAT_CONSTANT:
					case BOOLEAN_CONSTANT:
					case NULL_CONSTANT:
					case STRING_CONSTANT:
						{
						setState(580);
						constant();
						}
						break;
					case OPTIONAL:
						{
						setState(581);
						match(OPTIONAL);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
					setState(588);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(591);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprAtomContext extends ParserRuleContext {
		public ExprAtomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exprAtom; }
	 
		public ExprAtomContext() { }
		public void copyFrom(ExprAtomContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SqrtAtomContext extends ExprAtomContext {
		public TerminalNode SQRT() { return getToken(VtlParser.SQRT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public SqrtAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSqrtAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSqrtAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSqrtAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LtrimAtomContext extends ExprAtomContext {
		public TerminalNode LTRIM() { return getToken(VtlParser.LTRIM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public LtrimAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLtrimAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLtrimAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLtrimAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StockToFlowAtomContext extends ExprAtomContext {
		public TerminalNode STOCK_TO_FLOW() { return getToken(VtlParser.STOCK_TO_FLOW, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public StockToFlowAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterStockToFlowAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitStockToFlowAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitStockToFlowAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TrimAtomContext extends ExprAtomContext {
		public TerminalNode TRIM() { return getToken(VtlParser.TRIM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TrimAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTrimAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTrimAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTrimAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ValidateDPrulesetContext extends ExprAtomContext {
		public ValidationDatapointContext validationDatapoint() {
			return getRuleContext(ValidationDatapointContext.class,0);
		}
		public ValidateDPrulesetContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidateDPruleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidateDPruleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidateDPruleset(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InstrAtomContext extends ExprAtomContext {
		public TerminalNode INSTR() { return getToken(VtlParser.INSTR, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<OptionalExprContext> optionalExpr() {
			return getRuleContexts(OptionalExprContext.class);
		}
		public OptionalExprContext optionalExpr(int i) {
			return getRuleContext(OptionalExprContext.class,i);
		}
		public InstrAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterInstrAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitInstrAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitInstrAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CharsetMatchAtomContext extends ExprAtomContext {
		public TerminalNode CHARSET_MATCH() { return getToken(VtlParser.CHARSET_MATCH, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public CharsetMatchAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCharsetMatchAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCharsetMatchAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCharsetMatchAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LnAtomContext extends ExprAtomContext {
		public TerminalNode LN() { return getToken(VtlParser.LN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode TRUNC() { return getToken(VtlParser.TRUNC, 0); }
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public OptionalExprContext optionalExpr() {
			return getRuleContext(OptionalExprContext.class,0);
		}
		public LnAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLnAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLnAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLnAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ValidationSimpleContext extends ExprAtomContext {
		public ValidationExprContext validationExpr() {
			return getRuleContext(ValidationExprContext.class,0);
		}
		public ValidationSimpleContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidationSimple(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidationSimple(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidationSimple(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IsNullAtomContext extends ExprAtomContext {
		public TerminalNode ISNULL() { return getToken(VtlParser.ISNULL, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public IsNullAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterIsNullAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitIsNullAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitIsNullAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NvlAtomContext extends ExprAtomContext {
		public TerminalNode NVL() { return getToken(VtlParser.NVL, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public NvlAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterNvlAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitNvlAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitNvlAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CeilAtomContext extends ExprAtomContext {
		public TerminalNode CEIL() { return getToken(VtlParser.CEIL, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public CeilAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCeilAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCeilAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCeilAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RtrimAtomContext extends ExprAtomContext {
		public TerminalNode RTRIM() { return getToken(VtlParser.RTRIM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public RtrimAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRtrimAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRtrimAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRtrimAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RoundAtomContext extends ExprAtomContext {
		public TerminalNode ROUND() { return getToken(VtlParser.ROUND, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public OptionalExprContext optionalExpr() {
			return getRuleContext(OptionalExprContext.class,0);
		}
		public RoundAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRoundAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRoundAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRoundAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UcaseAtomContext extends ExprAtomContext {
		public TerminalNode UCASE() { return getToken(VtlParser.UCASE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public UcaseAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterUcaseAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitUcaseAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitUcaseAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LcaseAtomContext extends ExprAtomContext {
		public TerminalNode LCASE() { return getToken(VtlParser.LCASE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public LcaseAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLcaseAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLcaseAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLcaseAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class HierarchyExprAtomContext extends ExprAtomContext {
		public HierarchyExprContext hierarchyExpr() {
			return getRuleContext(HierarchyExprContext.class,0);
		}
		public HierarchyExprAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterHierarchyExprAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitHierarchyExprAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitHierarchyExprAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefAtomContext extends ExprAtomContext {
		public RefContext ref() {
			return getRuleContext(RefContext.class,0);
		}
		public RefAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRefAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRefAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRefAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FloorAtomContext extends ExprAtomContext {
		public TerminalNode FLOOR() { return getToken(VtlParser.FLOOR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public FloorAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterFloorAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitFloorAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitFloorAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubstrAtomContext extends ExprAtomContext {
		public TerminalNode SUBSTR() { return getToken(VtlParser.SUBSTR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<OptionalExprContext> optionalExpr() {
			return getRuleContexts(OptionalExprContext.class);
		}
		public OptionalExprContext optionalExpr(int i) {
			return getRuleContext(OptionalExprContext.class,i);
		}
		public SubstrAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSubstrAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSubstrAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSubstrAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogAtomContext extends ExprAtomContext {
		public TerminalNode LOG() { return getToken(VtlParser.LOG, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public LogAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLogAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLogAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLogAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PowerAtomContext extends ExprAtomContext {
		public TerminalNode POWER() { return getToken(VtlParser.POWER, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public PowerAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPowerAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPowerAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPowerAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LenAtomContext extends ExprAtomContext {
		public TerminalNode LEN() { return getToken(VtlParser.LEN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public LenAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLenAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLenAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLenAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MinAtomContext extends ExprAtomContext {
		public TerminalNode ABS() { return getToken(VtlParser.ABS, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public MinAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterMinAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitMinAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitMinAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ModAtomContext extends ExprAtomContext {
		public TerminalNode MOD() { return getToken(VtlParser.MOD, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public ModAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterModAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitModAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitModAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ValidateHRrulesetContext extends ExprAtomContext {
		public ValidationHierarchicalContext validationHierarchical() {
			return getRuleContext(ValidationHierarchicalContext.class,0);
		}
		public ValidateHRrulesetContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidateHRruleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidateHRruleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidateHRruleset(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BetweenAtomContext extends ExprAtomContext {
		public TerminalNode BETWEEN() { return getToken(VtlParser.BETWEEN, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public BetweenAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterBetweenAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitBetweenAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitBetweenAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExpAtomContext extends ExprAtomContext {
		public TerminalNode EXP() { return getToken(VtlParser.EXP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public ExpAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterExpAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitExpAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitExpAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class EvalExprAtomContext extends ExprAtomContext {
		public EvalExprContext evalExpr() {
			return getRuleContext(EvalExprContext.class,0);
		}
		public EvalExprAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterEvalExprAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitEvalExprAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitEvalExprAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastExprAtomContext extends ExprAtomContext {
		public CastExprContext castExpr() {
			return getRuleContext(CastExprContext.class,0);
		}
		public CastExprAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCastExprAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCastExprAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCastExprAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FlowToStockAtomContext extends ExprAtomContext {
		public TerminalNode FLOW_TO_STOCK() { return getToken(VtlParser.FLOW_TO_STOCK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public FlowToStockAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterFlowToStockAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitFlowToStockAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitFlowToStockAtom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ReplaceAtomContext extends ExprAtomContext {
		public TerminalNode REPLACE() { return getToken(VtlParser.REPLACE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public OptionalExprContext optionalExpr() {
			return getRuleContext(OptionalExprContext.class,0);
		}
		public ReplaceAtomContext(ExprAtomContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterReplaceAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitReplaceAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitReplaceAtom(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprAtomContext exprAtom() throws RecognitionException {
		ExprAtomContext _localctx = new ExprAtomContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_exprAtom);
		int _la;
		try {
			setState(776);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROUND:
				_localctx = new RoundAtomContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(593);
				match(ROUND);
				setState(594);
				match(T__12);
				setState(595);
				expr(0);
				setState(598);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(596);
					match(CARTESIAN_PER);
					setState(597);
					optionalExpr();
					}
				}

				setState(600);
				match(T__13);
				}
				break;
			case CEIL:
				_localctx = new CeilAtomContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(602);
				match(CEIL);
				setState(603);
				match(T__12);
				setState(604);
				expr(0);
				setState(605);
				match(T__13);
				}
				break;
			case FLOOR:
				_localctx = new FloorAtomContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(607);
				match(FLOOR);
				setState(608);
				match(T__12);
				setState(609);
				expr(0);
				setState(610);
				match(T__13);
				}
				break;
			case ABS:
				_localctx = new MinAtomContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(612);
				match(ABS);
				setState(613);
				match(T__12);
				setState(614);
				expr(0);
				setState(615);
				match(T__13);
				}
				break;
			case EXP:
				_localctx = new ExpAtomContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(617);
				match(EXP);
				setState(618);
				match(T__12);
				setState(619);
				expr(0);
				setState(620);
				match(T__13);
				}
				break;
			case LN:
				_localctx = new LnAtomContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(622);
				match(LN);
				setState(623);
				match(T__12);
				setState(624);
				expr(0);
				setState(625);
				match(T__13);
				}
				break;
			case LOG:
				_localctx = new LogAtomContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(627);
				match(LOG);
				setState(628);
				match(T__12);
				setState(629);
				expr(0);
				setState(630);
				match(CARTESIAN_PER);
				setState(631);
				expr(0);
				setState(632);
				match(T__13);
				}
				break;
			case TRUNC:
				_localctx = new LnAtomContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(634);
				match(TRUNC);
				setState(635);
				match(T__12);
				setState(636);
				expr(0);
				setState(639);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(637);
					match(CARTESIAN_PER);
					setState(638);
					optionalExpr();
					}
				}

				setState(641);
				match(T__13);
				}
				break;
			case POWER:
				_localctx = new PowerAtomContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(643);
				match(POWER);
				setState(644);
				match(T__12);
				setState(645);
				expr(0);
				setState(646);
				match(CARTESIAN_PER);
				setState(647);
				expr(0);
				setState(648);
				match(T__13);
				}
				break;
			case SQRT:
				_localctx = new SqrtAtomContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(650);
				match(SQRT);
				setState(651);
				match(T__12);
				setState(652);
				expr(0);
				setState(653);
				match(T__13);
				}
				break;
			case LEN:
				_localctx = new LenAtomContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(655);
				match(LEN);
				setState(656);
				match(T__12);
				setState(657);
				expr(0);
				setState(658);
				match(T__13);
				}
				break;
			case BETWEEN:
				_localctx = new BetweenAtomContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(660);
				match(BETWEEN);
				setState(661);
				match(T__12);
				setState(662);
				expr(0);
				setState(663);
				match(CARTESIAN_PER);
				setState(664);
				expr(0);
				setState(665);
				match(CARTESIAN_PER);
				setState(666);
				expr(0);
				setState(667);
				match(T__13);
				}
				break;
			case TRIM:
				_localctx = new TrimAtomContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(669);
				match(TRIM);
				setState(670);
				match(T__12);
				setState(671);
				expr(0);
				setState(672);
				match(T__13);
				}
				break;
			case LTRIM:
				_localctx = new LtrimAtomContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(674);
				match(LTRIM);
				setState(675);
				match(T__12);
				setState(676);
				expr(0);
				setState(677);
				match(T__13);
				}
				break;
			case RTRIM:
				_localctx = new RtrimAtomContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(679);
				match(RTRIM);
				setState(680);
				match(T__12);
				setState(681);
				expr(0);
				setState(682);
				match(T__13);
				}
				break;
			case UCASE:
				_localctx = new UcaseAtomContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(684);
				match(UCASE);
				setState(685);
				match(T__12);
				setState(686);
				expr(0);
				setState(687);
				match(T__13);
				}
				break;
			case LCASE:
				_localctx = new LcaseAtomContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(689);
				match(LCASE);
				setState(690);
				match(T__12);
				setState(691);
				expr(0);
				setState(692);
				match(T__13);
				}
				break;
			case SUBSTR:
				_localctx = new SubstrAtomContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(694);
				match(SUBSTR);
				setState(695);
				match(T__12);
				setState(696);
				expr(0);
				setState(699);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
				case 1:
					{
					setState(697);
					match(CARTESIAN_PER);
					setState(698);
					optionalExpr();
					}
					break;
				}
				setState(703);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(701);
					match(CARTESIAN_PER);
					setState(702);
					optionalExpr();
					}
				}

				setState(705);
				match(T__13);
				}
				break;
			case INSTR:
				_localctx = new InstrAtomContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(707);
				match(INSTR);
				setState(708);
				match(T__12);
				setState(709);
				expr(0);
				setState(710);
				match(CARTESIAN_PER);
				setState(711);
				expr(0);
				setState(714);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
				case 1:
					{
					setState(712);
					match(CARTESIAN_PER);
					setState(713);
					optionalExpr();
					}
					break;
				}
				setState(718);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(716);
					match(CARTESIAN_PER);
					setState(717);
					optionalExpr();
					}
				}

				setState(720);
				match(T__13);
				}
				break;
			case REPLACE:
				_localctx = new ReplaceAtomContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(722);
				match(REPLACE);
				setState(723);
				match(T__12);
				setState(724);
				expr(0);
				setState(725);
				match(CARTESIAN_PER);
				setState(726);
				expr(0);
				setState(729);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CARTESIAN_PER) {
					{
					setState(727);
					match(CARTESIAN_PER);
					setState(728);
					optionalExpr();
					}
				}

				setState(731);
				match(T__13);
				}
				break;
			case CHARSET_MATCH:
				_localctx = new CharsetMatchAtomContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(733);
				match(CHARSET_MATCH);
				setState(734);
				match(T__12);
				setState(735);
				expr(0);
				setState(736);
				match(CARTESIAN_PER);
				setState(737);
				expr(0);
				setState(738);
				match(T__13);
				}
				break;
			case ISNULL:
				_localctx = new IsNullAtomContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(740);
				match(ISNULL);
				setState(741);
				match(T__12);
				setState(742);
				expr(0);
				setState(743);
				match(T__13);
				}
				break;
			case NVL:
				_localctx = new NvlAtomContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(745);
				match(NVL);
				setState(746);
				match(T__12);
				setState(747);
				expr(0);
				setState(748);
				match(CARTESIAN_PER);
				setState(749);
				expr(0);
				setState(750);
				match(T__13);
				}
				break;
			case MOD:
				_localctx = new ModAtomContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(752);
				match(MOD);
				setState(753);
				match(T__12);
				setState(754);
				expr(0);
				setState(755);
				match(CARTESIAN_PER);
				setState(756);
				expr(0);
				setState(757);
				match(T__13);
				}
				break;
			case T__12:
			case INTEGER_CONSTANT:
			case FLOAT_CONSTANT:
			case BOOLEAN_CONSTANT:
			case NULL_CONSTANT:
			case STRING_CONSTANT:
			case IDENTIFIER:
				_localctx = new RefAtomContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(759);
				ref();
				}
				break;
			case EVAL:
				_localctx = new EvalExprAtomContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(760);
				evalExpr();
				}
				break;
			case CAST:
				_localctx = new CastExprAtomContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(761);
				castExpr();
				}
				break;
			case HIERARCHY:
				_localctx = new HierarchyExprAtomContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(762);
				hierarchyExpr();
				}
				break;
			case FLOW_TO_STOCK:
				_localctx = new FlowToStockAtomContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(763);
				match(FLOW_TO_STOCK);
				setState(764);
				match(T__12);
				setState(765);
				expr(0);
				setState(766);
				match(T__13);
				}
				break;
			case STOCK_TO_FLOW:
				_localctx = new StockToFlowAtomContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(768);
				match(STOCK_TO_FLOW);
				setState(769);
				match(T__12);
				setState(770);
				expr(0);
				setState(771);
				match(T__13);
				}
				break;
			case CHECK_DATAPOINT:
				_localctx = new ValidateDPrulesetContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(773);
				validationDatapoint();
				}
				break;
			case CHECK_HIERARCHY:
				_localctx = new ValidateHRrulesetContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(774);
				validationHierarchical();
				}
				break;
			case CHECK:
				_localctx = new ValidationSimpleContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(775);
				validationExpr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RefContext extends ParserRuleContext {
		public RefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ref; }
	 
		public RefContext() { }
		public void copyFrom(RefContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class VarIdRefContext extends RefContext {
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public VarIdRefContext(RefContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterVarIdRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitVarIdRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitVarIdRef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesisExprRefContext extends RefContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public ParenthesisExprRefContext(RefContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterParenthesisExprRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitParenthesisExprRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitParenthesisExprRef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConstantRefContext extends RefContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantRefContext(RefContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterConstantRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitConstantRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitConstantRef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RefContext ref() throws RecognitionException {
		RefContext _localctx = new RefContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_ref);
		try {
			setState(784);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__12:
				_localctx = new ParenthesisExprRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(778);
				match(T__12);
				setState(779);
				expr(0);
				setState(780);
				match(T__13);
				}
				break;
			case IDENTIFIER:
				_localctx = new VarIdRefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(782);
				varID();
				}
				break;
			case INTEGER_CONSTANT:
			case FLOAT_CONSTANT:
			case BOOLEAN_CONSTANT:
			case NULL_CONSTANT:
			case STRING_CONSTANT:
				_localctx = new ConstantRefContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(783);
				constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_identifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
			match(T__0);
			setState(787);
			match(IDENTIFIER);
			setState(792);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(788);
				match(CARTESIAN_PER);
				setState(789);
				match(IDENTIFIER);
				}
				}
				setState(794);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(795);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListsContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public ListsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLists(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListsContext lists() throws RecognitionException {
		ListsContext _localctx = new ListsContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_lists);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(797);
			match(T__15);
			setState(798);
			constant();
			setState(803);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(799);
				match(CARTESIAN_PER);
				setState(800);
				constant();
				}
				}
				setState(805);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(806);
			match(T__16);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EvalExprContext extends ParserRuleContext {
		public TerminalNode EVAL() { return getToken(VtlParser.EVAL, 0); }
		public RoutineNameContext routineName() {
			return getRuleContext(RoutineNameContext.class,0);
		}
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public TerminalNode LANGUAGE() { return getToken(VtlParser.LANGUAGE, 0); }
		public TerminalNode STRING_CONSTANT() { return getToken(VtlParser.STRING_CONSTANT, 0); }
		public TerminalNode RETURNS() { return getToken(VtlParser.RETURNS, 0); }
		public OutputParameterTypeContext outputParameterType() {
			return getRuleContext(OutputParameterTypeContext.class,0);
		}
		public EvalExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_evalExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterEvalExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitEvalExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitEvalExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EvalExprContext evalExpr() throws RecognitionException {
		EvalExprContext _localctx = new EvalExprContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_evalExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(808);
			match(EVAL);
			setState(809);
			match(T__12);
			setState(810);
			routineName();
			setState(811);
			match(T__12);
			setState(814);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(812);
				componentID();
				}
				break;
			case INTEGER_CONSTANT:
			case FLOAT_CONSTANT:
			case BOOLEAN_CONSTANT:
			case NULL_CONSTANT:
			case STRING_CONSTANT:
				{
				setState(813);
				constant();
				}
				break;
			case T__13:
			case CARTESIAN_PER:
				break;
			default:
				break;
			}
			setState(823);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(816);
				match(CARTESIAN_PER);
				setState(819);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case IDENTIFIER:
					{
					setState(817);
					componentID();
					}
					break;
				case INTEGER_CONSTANT:
				case FLOAT_CONSTANT:
				case BOOLEAN_CONSTANT:
				case NULL_CONSTANT:
				case STRING_CONSTANT:
					{
					setState(818);
					constant();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(825);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(826);
			match(T__13);
			setState(829);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LANGUAGE) {
				{
				setState(827);
				match(LANGUAGE);
				setState(828);
				match(STRING_CONSTANT);
				}
			}

			setState(833);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETURNS) {
				{
				setState(831);
				match(RETURNS);
				setState(832);
				outputParameterType();
				}
			}

			setState(835);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CastExprContext extends ParserRuleContext {
		public TerminalNode CAST() { return getToken(VtlParser.CAST, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public BasicScalarTypeContext basicScalarType() {
			return getRuleContext(BasicScalarTypeContext.class,0);
		}
		public ValueDomainNameContext valueDomainName() {
			return getRuleContext(ValueDomainNameContext.class,0);
		}
		public TerminalNode STRING_CONSTANT() { return getToken(VtlParser.STRING_CONSTANT, 0); }
		public CastExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_castExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCastExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCastExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCastExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CastExprContext castExpr() throws RecognitionException {
		CastExprContext _localctx = new CastExprContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_castExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(837);
			match(CAST);
			setState(838);
			match(T__12);
			setState(839);
			expr(0);
			setState(840);
			match(CARTESIAN_PER);
			setState(843);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BOOLEAN:
			case DATE:
			case TIME_PERIOD:
			case NUMBER:
			case STRING:
			case INTEGER:
			case DURATION:
			case SCALAR:
			case TIME:
				{
				setState(841);
				basicScalarType();
				}
				break;
			case IDENTIFIER:
				{
				setState(842);
				valueDomainName();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(847);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CARTESIAN_PER) {
				{
				setState(845);
				match(CARTESIAN_PER);
				setState(846);
				match(STRING_CONSTANT);
				}
			}

			setState(849);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PeriodExprContext extends ParserRuleContext {
		public TerminalNode PERIOD_INDICATOR() { return getToken(VtlParser.PERIOD_INDICATOR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public PeriodExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_periodExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPeriodExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPeriodExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPeriodExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PeriodExprContext periodExpr() throws RecognitionException {
		PeriodExprContext _localctx = new PeriodExprContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_periodExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(851);
			match(PERIOD_INDICATOR);
			setState(852);
			match(T__12);
			setState(854);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << T__12) | (1L << EVAL) | (1L << IF) | (1L << CURRENT_DATE) | (1L << NOT) | (1L << BETWEEN) | (1L << ISNULL) | (1L << UNION) | (1L << SYMDIFF) | (1L << INTERSECT) | (1L << CHECK) | (1L << EXISTS_IN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (RANK - 64)) | (1L << (MIN - 64)) | (1L << (MAX - 64)) | (1L << (ABS - 64)) | (1L << (LN - 64)) | (1L << (LOG - 64)) | (1L << (TRUNC - 64)) | (1L << (ROUND - 64)) | (1L << (POWER - 64)) | (1L << (MOD - 64)) | (1L << (LEN - 64)) | (1L << (TRIM - 64)) | (1L << (UCASE - 64)) | (1L << (LCASE - 64)) | (1L << (SUBSTR - 64)) | (1L << (SUM - 64)) | (1L << (AVG - 64)) | (1L << (MEDIAN - 64)) | (1L << (COUNT - 64)) | (1L << (EXP - 64)) | (1L << (CHARSET_MATCH - 64)) | (1L << (NVL - 64)) | (1L << (HIERARCHY - 64)) | (1L << (DEFINE - 64)) | (1L << (LTRIM - 64)) | (1L << (RTRIM - 64)) | (1L << (INSTR - 64)) | (1L << (REPLACE - 64)) | (1L << (CEIL - 64)) | (1L << (FLOOR - 64)) | (1L << (SQRT - 64)) | (1L << (SETDIFF - 64)) | (1L << (STDDEV_POP - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STDDEV_SAMP - 128)) | (1L << (VAR_POP - 128)) | (1L << (VAR_SAMP - 128)) | (1L << (FIRST_VALUE - 128)) | (1L << (LAST_VALUE - 128)) | (1L << (LAG - 128)) | (1L << (LEAD - 128)) | (1L << (RATIO_TO_REPORT - 128)) | (1L << (FILL_TIME_SERIES - 128)) | (1L << (FLOW_TO_STOCK - 128)) | (1L << (STOCK_TO_FLOW - 128)) | (1L << (TIMESHIFT - 128)) | (1L << (INNER_JOIN - 128)) | (1L << (LEFT_JOIN - 128)) | (1L << (CROSS_JOIN - 128)) | (1L << (FULL_JOIN - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PERIOD_INDICATOR - 193)) | (1L << (TIME_AGG - 193)) | (1L << (CAST - 193)) | (1L << (CHECK_DATAPOINT - 193)) | (1L << (CHECK_HIERARCHY - 193)) | (1L << (INTEGER_CONSTANT - 193)) | (1L << (FLOAT_CONSTANT - 193)) | (1L << (BOOLEAN_CONSTANT - 193)) | (1L << (NULL_CONSTANT - 193)) | (1L << (STRING_CONSTANT - 193)) | (1L << (IDENTIFIER - 193)))) != 0)) {
				{
				setState(853);
				expr(0);
				}
			}

			setState(856);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TimeShiftExprContext extends ParserRuleContext {
		public TerminalNode TIMESHIFT() { return getToken(VtlParser.TIMESHIFT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public TerminalNode INTEGER_CONSTANT() { return getToken(VtlParser.INTEGER_CONSTANT, 0); }
		public TimeShiftExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeShiftExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTimeShiftExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTimeShiftExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTimeShiftExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeShiftExprContext timeShiftExpr() throws RecognitionException {
		TimeShiftExprContext _localctx = new TimeShiftExprContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_timeShiftExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(858);
			match(TIMESHIFT);
			setState(859);
			match(T__12);
			setState(860);
			expr(0);
			setState(861);
			match(CARTESIAN_PER);
			setState(862);
			match(INTEGER_CONSTANT);
			setState(863);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TimeSeriesExprContext extends ParserRuleContext {
		public TerminalNode FILL_TIME_SERIES() { return getToken(VtlParser.FILL_TIME_SERIES, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public TerminalNode SINGLE() { return getToken(VtlParser.SINGLE, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public TimeSeriesExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeSeriesExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTimeSeriesExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTimeSeriesExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTimeSeriesExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeSeriesExprContext timeSeriesExpr() throws RecognitionException {
		TimeSeriesExprContext _localctx = new TimeSeriesExprContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_timeSeriesExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(865);
			match(FILL_TIME_SERIES);
			setState(866);
			match(T__12);
			setState(867);
			expr(0);
			setState(870);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CARTESIAN_PER) {
				{
				setState(868);
				match(CARTESIAN_PER);
				setState(869);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==SINGLE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(872);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TimeAggExprContext extends ParserRuleContext {
		public TerminalNode TIME_AGG() { return getToken(VtlParser.TIME_AGG, 0); }
		public List<TerminalNode> STRING_CONSTANT() { return getTokens(VtlParser.STRING_CONSTANT); }
		public TerminalNode STRING_CONSTANT(int i) {
			return getToken(VtlParser.STRING_CONSTANT, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<TerminalNode> OPTIONAL() { return getTokens(VtlParser.OPTIONAL); }
		public TerminalNode OPTIONAL(int i) {
			return getToken(VtlParser.OPTIONAL, i);
		}
		public TerminalNode FIRST() { return getToken(VtlParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(VtlParser.LAST, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TimeAggExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeAggExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterTimeAggExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitTimeAggExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitTimeAggExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeAggExprContext timeAggExpr() throws RecognitionException {
		TimeAggExprContext _localctx = new TimeAggExprContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_timeAggExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(874);
			match(TIME_AGG);
			setState(875);
			match(T__12);
			setState(876);
			match(STRING_CONSTANT);
			setState(879);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				{
				setState(877);
				match(CARTESIAN_PER);
				setState(878);
				_la = _input.LA(1);
				if ( !(_la==OPTIONAL || _la==STRING_CONSTANT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			setState(886);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				setState(881);
				match(CARTESIAN_PER);
				setState(884);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case T__2:
				case T__3:
				case T__12:
				case EVAL:
				case IF:
				case CURRENT_DATE:
				case NOT:
				case BETWEEN:
				case ISNULL:
				case UNION:
				case SYMDIFF:
				case INTERSECT:
				case CHECK:
				case EXISTS_IN:
				case RANK:
				case MIN:
				case MAX:
				case ABS:
				case LN:
				case LOG:
				case TRUNC:
				case ROUND:
				case POWER:
				case MOD:
				case LEN:
				case TRIM:
				case UCASE:
				case LCASE:
				case SUBSTR:
				case SUM:
				case AVG:
				case MEDIAN:
				case COUNT:
				case EXP:
				case CHARSET_MATCH:
				case NVL:
				case HIERARCHY:
				case DEFINE:
				case LTRIM:
				case RTRIM:
				case INSTR:
				case REPLACE:
				case CEIL:
				case FLOOR:
				case SQRT:
				case SETDIFF:
				case STDDEV_POP:
				case STDDEV_SAMP:
				case VAR_POP:
				case VAR_SAMP:
				case FIRST_VALUE:
				case LAST_VALUE:
				case LAG:
				case LEAD:
				case RATIO_TO_REPORT:
				case FILL_TIME_SERIES:
				case FLOW_TO_STOCK:
				case STOCK_TO_FLOW:
				case TIMESHIFT:
				case INNER_JOIN:
				case LEFT_JOIN:
				case CROSS_JOIN:
				case FULL_JOIN:
				case PERIOD_INDICATOR:
				case TIME_AGG:
				case CAST:
				case CHECK_DATAPOINT:
				case CHECK_HIERARCHY:
				case INTEGER_CONSTANT:
				case FLOAT_CONSTANT:
				case BOOLEAN_CONSTANT:
				case NULL_CONSTANT:
				case STRING_CONSTANT:
				case IDENTIFIER:
					{
					setState(882);
					expr(0);
					}
					break;
				case OPTIONAL:
					{
					setState(883);
					match(OPTIONAL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			setState(890);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CARTESIAN_PER) {
				{
				setState(888);
				match(CARTESIAN_PER);
				setState(889);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(892);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValidationExprContext extends ParserRuleContext {
		public TerminalNode CHECK() { return getToken(VtlParser.CHECK, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public ErCodeContext erCode() {
			return getRuleContext(ErCodeContext.class,0);
		}
		public ErLevelContext erLevel() {
			return getRuleContext(ErLevelContext.class,0);
		}
		public TerminalNode IMBALANCE() { return getToken(VtlParser.IMBALANCE, 0); }
		public TerminalNode INVALID() { return getToken(VtlParser.INVALID, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public ValidationExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_validationExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidationExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidationExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidationExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValidationExprContext validationExpr() throws RecognitionException {
		ValidationExprContext _localctx = new ValidationExprContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_validationExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(894);
			match(CHECK);
			setState(895);
			match(T__12);
			setState(896);
			expr(0);
			setState(898);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORCODE) {
				{
				setState(897);
				erCode();
				}
			}

			setState(901);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ERRORLEVEL) {
				{
				setState(900);
				erLevel();
				}
			}

			setState(905);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IMBALANCE) {
				{
				setState(903);
				match(IMBALANCE);
				setState(904);
				expr(0);
				}
			}

			setState(908);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ALL || _la==INVALID) {
				{
				setState(907);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==INVALID) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(910);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValidationDatapointContext extends ParserRuleContext {
		public TerminalNode CHECK_DATAPOINT() { return getToken(VtlParser.CHECK_DATAPOINT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public TerminalNode COMPONENTS() { return getToken(VtlParser.COMPONENTS, 0); }
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public TerminalNode INVALID() { return getToken(VtlParser.INVALID, 0); }
		public TerminalNode ALL_MEASURES() { return getToken(VtlParser.ALL_MEASURES, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public ValidationDatapointContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_validationDatapoint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidationDatapoint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidationDatapoint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidationDatapoint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValidationDatapointContext validationDatapoint() throws RecognitionException {
		ValidationDatapointContext _localctx = new ValidationDatapointContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_validationDatapoint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(912);
			match(CHECK_DATAPOINT);
			setState(913);
			match(T__12);
			setState(914);
			expr(0);
			setState(915);
			match(CARTESIAN_PER);
			setState(916);
			match(IDENTIFIER);
			setState(926);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMPONENTS) {
				{
				setState(917);
				match(COMPONENTS);
				setState(918);
				componentID();
				setState(923);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(919);
					match(CARTESIAN_PER);
					setState(920);
					componentID();
					}
					}
					setState(925);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(929);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ALL || _la==INVALID || _la==ALL_MEASURES) {
				{
				setState(928);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==INVALID || _la==ALL_MEASURES) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(931);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValidationHierarchicalContext extends ParserRuleContext {
		public TerminalNode CHECK_HIERARCHY() { return getToken(VtlParser.CHECK_HIERARCHY, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public TerminalNode CONDITION() { return getToken(VtlParser.CONDITION, 0); }
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public TerminalNode RULE() { return getToken(VtlParser.RULE, 0); }
		public TerminalNode NON_NULL() { return getToken(VtlParser.NON_NULL, 0); }
		public TerminalNode NON_ZERO() { return getToken(VtlParser.NON_ZERO, 0); }
		public TerminalNode PARTIAL_NULL() { return getToken(VtlParser.PARTIAL_NULL, 0); }
		public TerminalNode PARTIAL_ZERO() { return getToken(VtlParser.PARTIAL_ZERO, 0); }
		public TerminalNode ALWAYS_NULL() { return getToken(VtlParser.ALWAYS_NULL, 0); }
		public TerminalNode ALWAYS_ZERO() { return getToken(VtlParser.ALWAYS_ZERO, 0); }
		public TerminalNode DATASET() { return getToken(VtlParser.DATASET, 0); }
		public TerminalNode DATASET_PRIORITY() { return getToken(VtlParser.DATASET_PRIORITY, 0); }
		public TerminalNode INVALID() { return getToken(VtlParser.INVALID, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public TerminalNode ALL_MEASURES() { return getToken(VtlParser.ALL_MEASURES, 0); }
		public ValidationHierarchicalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_validationHierarchical; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValidationHierarchical(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValidationHierarchical(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValidationHierarchical(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValidationHierarchicalContext validationHierarchical() throws RecognitionException {
		ValidationHierarchicalContext _localctx = new ValidationHierarchicalContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_validationHierarchical);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(933);
			match(CHECK_HIERARCHY);
			setState(934);
			match(T__12);
			setState(935);
			expr(0);
			setState(936);
			match(CARTESIAN_PER);
			setState(937);
			match(IDENTIFIER);
			setState(947);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CONDITION) {
				{
				setState(938);
				match(CONDITION);
				setState(939);
				componentID();
				setState(944);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(940);
					match(CARTESIAN_PER);
					setState(941);
					componentID();
					}
					}
					setState(946);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(951);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RULE) {
				{
				setState(949);
				match(RULE);
				setState(950);
				match(IDENTIFIER);
				}
			}

			setState(954);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & ((1L << (NON_NULL - 210)) | (1L << (NON_ZERO - 210)) | (1L << (PARTIAL_NULL - 210)) | (1L << (PARTIAL_ZERO - 210)) | (1L << (ALWAYS_NULL - 210)) | (1L << (ALWAYS_ZERO - 210)))) != 0)) {
				{
				setState(953);
				_la = _input.LA(1);
				if ( !(((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & ((1L << (NON_NULL - 210)) | (1L << (NON_ZERO - 210)) | (1L << (PARTIAL_NULL - 210)) | (1L << (PARTIAL_ZERO - 210)) | (1L << (ALWAYS_NULL - 210)) | (1L << (ALWAYS_ZERO - 210)))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(957);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DATASET || _la==DATASET_PRIORITY) {
				{
				setState(956);
				_la = _input.LA(1);
				if ( !(_la==DATASET || _la==DATASET_PRIORITY) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(960);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ALL || _la==INVALID || _la==ALL_MEASURES) {
				{
				setState(959);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==INVALID || _la==ALL_MEASURES) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(962);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErCodeContext extends ParserRuleContext {
		public TerminalNode ERRORCODE() { return getToken(VtlParser.ERRORCODE, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ErCodeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_erCode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterErCode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitErCode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitErCode(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErCodeContext erCode() throws RecognitionException {
		ErCodeContext _localctx = new ErCodeContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_erCode);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(964);
			match(ERRORCODE);
			setState(965);
			constant();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErLevelContext extends ParserRuleContext {
		public TerminalNode ERRORLEVEL() { return getToken(VtlParser.ERRORLEVEL, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ErLevelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_erLevel; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterErLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitErLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitErLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErLevelContext erLevel() throws RecognitionException {
		ErLevelContext _localctx = new ErLevelContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_erLevel);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(967);
			match(ERRORLEVEL);
			setState(968);
			constant();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HierarchyExprContext extends ParserRuleContext {
		public TerminalNode HIERARCHY() { return getToken(VtlParser.HIERARCHY, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public TerminalNode CONDITION() { return getToken(VtlParser.CONDITION, 0); }
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<TerminalNode> RULE() { return getTokens(VtlParser.RULE); }
		public TerminalNode RULE(int i) {
			return getToken(VtlParser.RULE, i);
		}
		public List<TerminalNode> OPTIONAL() { return getTokens(VtlParser.OPTIONAL); }
		public TerminalNode OPTIONAL(int i) {
			return getToken(VtlParser.OPTIONAL, i);
		}
		public TerminalNode NON_NULL() { return getToken(VtlParser.NON_NULL, 0); }
		public TerminalNode NON_ZERO() { return getToken(VtlParser.NON_ZERO, 0); }
		public TerminalNode PARTIAL_NULL() { return getToken(VtlParser.PARTIAL_NULL, 0); }
		public TerminalNode PARTIAL_ZERO() { return getToken(VtlParser.PARTIAL_ZERO, 0); }
		public TerminalNode ALWAYS_NULL() { return getToken(VtlParser.ALWAYS_NULL, 0); }
		public TerminalNode ALWAYS_ZERO() { return getToken(VtlParser.ALWAYS_ZERO, 0); }
		public TerminalNode DATASET() { return getToken(VtlParser.DATASET, 0); }
		public TerminalNode RULE_PRIORITY() { return getToken(VtlParser.RULE_PRIORITY, 0); }
		public TerminalNode COMPUTED() { return getToken(VtlParser.COMPUTED, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public HierarchyExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hierarchyExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterHierarchyExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitHierarchyExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitHierarchyExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HierarchyExprContext hierarchyExpr() throws RecognitionException {
		HierarchyExprContext _localctx = new HierarchyExprContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_hierarchyExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(970);
			match(HIERARCHY);
			setState(971);
			match(T__12);
			setState(972);
			expr(0);
			setState(973);
			match(CARTESIAN_PER);
			setState(974);
			match(IDENTIFIER);
			setState(984);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CONDITION) {
				{
				setState(975);
				match(CONDITION);
				setState(976);
				componentID();
				setState(981);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(977);
					match(CARTESIAN_PER);
					setState(978);
					componentID();
					}
					}
					setState(983);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(988);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
			case 1:
				{
				setState(986);
				match(RULE);
				setState(987);
				match(IDENTIFIER);
				}
				break;
			}
			setState(992);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
			case 1:
				{
				setState(990);
				_la = _input.LA(1);
				if ( !(((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & ((1L << (NON_NULL - 210)) | (1L << (NON_ZERO - 210)) | (1L << (PARTIAL_NULL - 210)) | (1L << (PARTIAL_ZERO - 210)) | (1L << (ALWAYS_NULL - 210)) | (1L << (ALWAYS_ZERO - 210)))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				setState(991);
				match(OPTIONAL);
				}
				break;
			}
			setState(996);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
			case 1:
				{
				setState(994);
				_la = _input.LA(1);
				if ( !(_la==DATASET || _la==RULE || _la==RULE_PRIORITY) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				setState(995);
				match(OPTIONAL);
				}
				break;
			}
			setState(1000);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
			case COMPUTED:
				{
				setState(998);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==COMPUTED) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case OPTIONAL:
				{
				setState(999);
				match(OPTIONAL);
				}
				break;
			case T__13:
				break;
			default:
				break;
			}
			setState(1002);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasetClauseContext extends ParserRuleContext {
		public TerminalNode RENAME() { return getToken(VtlParser.RENAME, 0); }
		public RenameClauseContext renameClause() {
			return getRuleContext(RenameClauseContext.class,0);
		}
		public AggrClauseContext aggrClause() {
			return getRuleContext(AggrClauseContext.class,0);
		}
		public FilterClauseContext filterClause() {
			return getRuleContext(FilterClauseContext.class,0);
		}
		public CalcClauseContext calcClause() {
			return getRuleContext(CalcClauseContext.class,0);
		}
		public KeepClauseContext keepClause() {
			return getRuleContext(KeepClauseContext.class,0);
		}
		public DropClauseContext dropClause() {
			return getRuleContext(DropClauseContext.class,0);
		}
		public PivotExprContext pivotExpr() {
			return getRuleContext(PivotExprContext.class,0);
		}
		public UnpivotExprContext unpivotExpr() {
			return getRuleContext(UnpivotExprContext.class,0);
		}
		public SubspaceExprContext subspaceExpr() {
			return getRuleContext(SubspaceExprContext.class,0);
		}
		public DatasetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasetClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDatasetClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDatasetClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDatasetClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasetClauseContext datasetClause() throws RecognitionException {
		DatasetClauseContext _localctx = new DatasetClauseContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_datasetClause);
		try {
			setState(1014);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case RENAME:
				enterOuterAlt(_localctx, 1);
				{
				setState(1004);
				match(RENAME);
				setState(1005);
				renameClause();
				}
				break;
			case AGGREGATE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1006);
				aggrClause();
				}
				break;
			case FILTER:
				enterOuterAlt(_localctx, 3);
				{
				setState(1007);
				filterClause();
				}
				break;
			case CALC:
				enterOuterAlt(_localctx, 4);
				{
				setState(1008);
				calcClause();
				}
				break;
			case KEEP:
				enterOuterAlt(_localctx, 5);
				{
				setState(1009);
				keepClause();
				}
				break;
			case DROP:
				enterOuterAlt(_localctx, 6);
				{
				setState(1010);
				dropClause();
				}
				break;
			case PIVOT:
				enterOuterAlt(_localctx, 7);
				{
				setState(1011);
				pivotExpr();
				}
				break;
			case UNPIVOT:
				enterOuterAlt(_localctx, 8);
				{
				setState(1012);
				unpivotExpr();
				}
				break;
			case SUBSPACE:
				enterOuterAlt(_localctx, 9);
				{
				setState(1013);
				subspaceExpr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnFunctionClauseContext extends ParserRuleContext {
		public TerminalNode OVER() { return getToken(VtlParser.OVER, 0); }
		public AggrFunctionNameContext aggrFunctionName() {
			return getRuleContext(AggrFunctionNameContext.class,0);
		}
		public AnFunctionContext anFunction() {
			return getRuleContext(AnFunctionContext.class,0);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public PartitionByClauseContext partitionByClause() {
			return getRuleContext(PartitionByClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public WindowingClauseContext windowingClause() {
			return getRuleContext(WindowingClauseContext.class,0);
		}
		public AnFunctionClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anFunctionClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAnFunctionClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAnFunctionClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAnFunctionClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnFunctionClauseContext anFunctionClause() throws RecognitionException {
		AnFunctionClauseContext _localctx = new AnFunctionClauseContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_anFunctionClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1018);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MIN:
			case MAX:
			case SUM:
			case AVG:
			case MEDIAN:
			case COUNT:
			case STDDEV_POP:
			case STDDEV_SAMP:
			case VAR_POP:
			case VAR_SAMP:
				{
				setState(1016);
				aggrFunctionName();
				}
				break;
			case RANK:
			case FIRST_VALUE:
			case LAST_VALUE:
			case LAG:
			case LEAD:
			case RATIO_TO_REPORT:
				{
				setState(1017);
				anFunction();
				}
				break;
			case T__12:
				break;
			default:
				break;
			}
			setState(1020);
			match(T__12);
			setState(1022);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << T__12) | (1L << EVAL) | (1L << IF) | (1L << CURRENT_DATE) | (1L << NOT) | (1L << BETWEEN) | (1L << ISNULL) | (1L << UNION) | (1L << SYMDIFF) | (1L << INTERSECT) | (1L << CHECK) | (1L << EXISTS_IN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (RANK - 64)) | (1L << (MIN - 64)) | (1L << (MAX - 64)) | (1L << (ABS - 64)) | (1L << (LN - 64)) | (1L << (LOG - 64)) | (1L << (TRUNC - 64)) | (1L << (ROUND - 64)) | (1L << (POWER - 64)) | (1L << (MOD - 64)) | (1L << (LEN - 64)) | (1L << (TRIM - 64)) | (1L << (UCASE - 64)) | (1L << (LCASE - 64)) | (1L << (SUBSTR - 64)) | (1L << (SUM - 64)) | (1L << (AVG - 64)) | (1L << (MEDIAN - 64)) | (1L << (COUNT - 64)) | (1L << (EXP - 64)) | (1L << (CHARSET_MATCH - 64)) | (1L << (NVL - 64)) | (1L << (HIERARCHY - 64)) | (1L << (DEFINE - 64)) | (1L << (LTRIM - 64)) | (1L << (RTRIM - 64)) | (1L << (INSTR - 64)) | (1L << (REPLACE - 64)) | (1L << (CEIL - 64)) | (1L << (FLOOR - 64)) | (1L << (SQRT - 64)) | (1L << (SETDIFF - 64)) | (1L << (STDDEV_POP - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STDDEV_SAMP - 128)) | (1L << (VAR_POP - 128)) | (1L << (VAR_SAMP - 128)) | (1L << (FIRST_VALUE - 128)) | (1L << (LAST_VALUE - 128)) | (1L << (LAG - 128)) | (1L << (LEAD - 128)) | (1L << (RATIO_TO_REPORT - 128)) | (1L << (FILL_TIME_SERIES - 128)) | (1L << (FLOW_TO_STOCK - 128)) | (1L << (STOCK_TO_FLOW - 128)) | (1L << (TIMESHIFT - 128)) | (1L << (INNER_JOIN - 128)) | (1L << (LEFT_JOIN - 128)) | (1L << (CROSS_JOIN - 128)) | (1L << (FULL_JOIN - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PERIOD_INDICATOR - 193)) | (1L << (TIME_AGG - 193)) | (1L << (CAST - 193)) | (1L << (CHECK_DATAPOINT - 193)) | (1L << (CHECK_HIERARCHY - 193)) | (1L << (INTEGER_CONSTANT - 193)) | (1L << (FLOAT_CONSTANT - 193)) | (1L << (BOOLEAN_CONSTANT - 193)) | (1L << (NULL_CONSTANT - 193)) | (1L << (STRING_CONSTANT - 193)) | (1L << (IDENTIFIER - 193)))) != 0)) {
				{
				setState(1021);
				expr(0);
				}
			}

			setState(1028);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1024);
				match(CARTESIAN_PER);
				setState(1025);
				expr(0);
				}
				}
				setState(1030);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1031);
			match(OVER);
			setState(1032);
			match(T__12);
			setState(1034);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1033);
				partitionByClause();
				}
			}

			setState(1037);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1036);
				orderByClause();
				}
			}

			setState(1040);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DATA || _la==RANGE) {
				{
				setState(1039);
				windowingClause();
				}
			}

			setState(1042);
			match(T__13);
			setState(1043);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionByClauseContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(VtlParser.PARTITION, 0); }
		public TerminalNode BY() { return getToken(VtlParser.BY, 0); }
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public PartitionByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPartitionByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPartitionByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPartitionByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionByClauseContext partitionByClause() throws RecognitionException {
		PartitionByClauseContext _localctx = new PartitionByClauseContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_partitionByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1045);
			match(PARTITION);
			setState(1046);
			match(BY);
			setState(1047);
			match(IDENTIFIER);
			setState(1052);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1048);
				match(CARTESIAN_PER);
				setState(1049);
				match(IDENTIFIER);
				}
				}
				setState(1054);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderByClauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(VtlParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(VtlParser.BY, 0); }
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public List<TerminalNode> ASC() { return getTokens(VtlParser.ASC); }
		public TerminalNode ASC(int i) {
			return getToken(VtlParser.ASC, i);
		}
		public List<TerminalNode> DESC() { return getTokens(VtlParser.DESC); }
		public TerminalNode DESC(int i) {
			return getToken(VtlParser.DESC, i);
		}
		public OrderByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterOrderByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitOrderByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitOrderByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderByClauseContext orderByClause() throws RecognitionException {
		OrderByClauseContext _localctx = new OrderByClauseContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_orderByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1055);
			match(ORDER);
			setState(1056);
			match(BY);
			setState(1057);
			componentID();
			setState(1059);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1058);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1068);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1061);
				match(CARTESIAN_PER);
				setState(1062);
				componentID();
				setState(1064);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASC || _la==DESC) {
					{
					setState(1063);
					_la = _input.LA(1);
					if ( !(_la==ASC || _la==DESC) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				}
				setState(1070);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowingClauseContext extends ParserRuleContext {
		public TerminalNode BETWEEN() { return getToken(VtlParser.BETWEEN, 0); }
		public List<LimitClauseItemContext> limitClauseItem() {
			return getRuleContexts(LimitClauseItemContext.class);
		}
		public LimitClauseItemContext limitClauseItem(int i) {
			return getRuleContext(LimitClauseItemContext.class,i);
		}
		public TerminalNode AND() { return getToken(VtlParser.AND, 0); }
		public TerminalNode RANGE() { return getToken(VtlParser.RANGE, 0); }
		public TerminalNode DATA() { return getToken(VtlParser.DATA, 0); }
		public TerminalNode POINTS() { return getToken(VtlParser.POINTS, 0); }
		public WindowingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterWindowingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitWindowingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitWindowingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowingClauseContext windowingClause() throws RecognitionException {
		WindowingClauseContext _localctx = new WindowingClauseContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_windowingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1074);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DATA:
				{
				{
				setState(1071);
				match(DATA);
				setState(1072);
				match(POINTS);
				}
				}
				break;
			case RANGE:
				{
				setState(1073);
				match(RANGE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1076);
			match(BETWEEN);
			setState(1077);
			limitClauseItem();
			setState(1078);
			match(AND);
			setState(1079);
			limitClauseItem();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LimitClauseItemContext extends ParserRuleContext {
		public TerminalNode INTEGER_CONSTANT() { return getToken(VtlParser.INTEGER_CONSTANT, 0); }
		public TerminalNode PRECEDING() { return getToken(VtlParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(VtlParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(VtlParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(VtlParser.DATA, 0); }
		public TerminalNode POINT() { return getToken(VtlParser.POINT, 0); }
		public TerminalNode UNBOUNDED() { return getToken(VtlParser.UNBOUNDED, 0); }
		public LimitClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLimitClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLimitClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLimitClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitClauseItemContext limitClauseItem() throws RecognitionException {
		LimitClauseItemContext _localctx = new LimitClauseItemContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_limitClauseItem);
		try {
			setState(1092);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1081);
				match(INTEGER_CONSTANT);
				setState(1082);
				match(PRECEDING);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1083);
				match(INTEGER_CONSTANT);
				setState(1084);
				match(FOLLOWING);
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1085);
				match(CURRENT);
				setState(1086);
				match(DATA);
				setState(1087);
				match(POINT);
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(1088);
				match(UNBOUNDED);
				setState(1089);
				match(PRECEDING);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(1090);
				match(UNBOUNDED);
				setState(1091);
				match(FOLLOWING);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinExprContext extends ParserRuleContext {
		public JoinKeywordContext joinKeyword() {
			return getRuleContext(JoinKeywordContext.class,0);
		}
		public JoinClauseContext joinClause() {
			return getRuleContext(JoinClauseContext.class,0);
		}
		public JoinBodyContext joinBody() {
			return getRuleContext(JoinBodyContext.class,0);
		}
		public JoinExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinExprContext joinExpr() throws RecognitionException {
		JoinExprContext _localctx = new JoinExprContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_joinExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1094);
			joinKeyword();
			setState(1095);
			match(T__12);
			setState(1096);
			joinClause();
			setState(1098);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,106,_ctx) ) {
			case 1:
				{
				setState(1097);
				joinBody();
				}
				break;
			}
			setState(1100);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinClauseContext extends ParserRuleContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> AS() { return getTokens(VtlParser.AS); }
		public TerminalNode AS(int i) {
			return getToken(VtlParser.AS, i);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public TerminalNode USING() { return getToken(VtlParser.USING, 0); }
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public JoinClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinClauseContext joinClause() throws RecognitionException {
		JoinClauseContext _localctx = new JoinClauseContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_joinClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1102);
			expr(0);
			setState(1105);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1103);
				match(AS);
				setState(1104);
				match(IDENTIFIER);
				}
			}

			setState(1115);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1107);
				match(CARTESIAN_PER);
				setState(1108);
				expr(0);
				setState(1111);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1109);
					match(AS);
					setState(1110);
					match(IDENTIFIER);
					}
				}

				}
				}
				setState(1117);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1127);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==USING) {
				{
				setState(1118);
				match(USING);
				setState(1119);
				componentID();
				setState(1124);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(1120);
					match(CARTESIAN_PER);
					setState(1121);
					componentID();
					}
					}
					setState(1126);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinBodyContext extends ParserRuleContext {
		public JoinFilterClauseContext joinFilterClause() {
			return getRuleContext(JoinFilterClauseContext.class,0);
		}
		public JoinCalcClauseContext joinCalcClause() {
			return getRuleContext(JoinCalcClauseContext.class,0);
		}
		public JoinApplyClauseContext joinApplyClause() {
			return getRuleContext(JoinApplyClauseContext.class,0);
		}
		public JoinAggClauseContext joinAggClause() {
			return getRuleContext(JoinAggClauseContext.class,0);
		}
		public JoinKeepClauseContext joinKeepClause() {
			return getRuleContext(JoinKeepClauseContext.class,0);
		}
		public JoinDropClauseContext joinDropClause() {
			return getRuleContext(JoinDropClauseContext.class,0);
		}
		public JoinRenameClauseContext joinRenameClause() {
			return getRuleContext(JoinRenameClauseContext.class,0);
		}
		public JoinBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinBodyContext joinBody() throws RecognitionException {
		JoinBodyContext _localctx = new JoinBodyContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_joinBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1130);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FILTER || _la==IDENTIFIER) {
				{
				setState(1129);
				joinFilterClause();
				}
			}

			setState(1135);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
			case 1:
				{
				setState(1132);
				joinCalcClause();
				}
				break;
			case 2:
				{
				setState(1133);
				joinApplyClause();
				}
				break;
			case 3:
				{
				setState(1134);
				joinAggClause();
				}
				break;
			}
			setState(1139);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case KEEP:
				{
				setState(1137);
				joinKeepClause();
				}
				break;
			case DROP:
				{
				setState(1138);
				joinDropClause();
				}
				break;
			case T__13:
			case RENAME:
				break;
			default:
				break;
			}
			setState(1142);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RENAME) {
				{
				setState(1141);
				joinRenameClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCalcClauseContext extends ParserRuleContext {
		public List<JoinCalcClauseItemContext> joinCalcClauseItem() {
			return getRuleContexts(JoinCalcClauseItemContext.class);
		}
		public JoinCalcClauseItemContext joinCalcClauseItem(int i) {
			return getRuleContext(JoinCalcClauseItemContext.class,i);
		}
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinCalcClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCalcClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinCalcClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinCalcClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinCalcClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCalcClauseContext joinCalcClause() throws RecognitionException {
		JoinCalcClauseContext _localctx = new JoinCalcClauseContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_joinCalcClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1145);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1144);
				componentRole();
				}
			}

			setState(1147);
			joinCalcClauseItem();
			setState(1152);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1148);
				match(CARTESIAN_PER);
				setState(1149);
				joinCalcClauseItem();
				}
				}
				setState(1154);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCalcClauseItemContext extends ParserRuleContext {
		public TerminalNode CALC() { return getToken(VtlParser.CALC, 0); }
		public List<JoinCalcExprContext> joinCalcExpr() {
			return getRuleContexts(JoinCalcExprContext.class);
		}
		public JoinCalcExprContext joinCalcExpr(int i) {
			return getRuleContext(JoinCalcExprContext.class,i);
		}
		public List<ComponentRoleContext> componentRole() {
			return getRuleContexts(ComponentRoleContext.class);
		}
		public ComponentRoleContext componentRole(int i) {
			return getRuleContext(ComponentRoleContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinCalcClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCalcClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinCalcClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinCalcClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinCalcClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCalcClauseItemContext joinCalcClauseItem() throws RecognitionException {
		JoinCalcClauseItemContext _localctx = new JoinCalcClauseItemContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_joinCalcClauseItem);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1155);
			match(CALC);
			setState(1157);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1156);
				componentRole();
				}
			}

			setState(1159);
			joinCalcExpr();
			setState(1167);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1160);
					match(CARTESIAN_PER);
					setState(1162);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
						{
						setState(1161);
						componentRole();
						}
					}

					setState(1164);
					joinCalcExpr();
					}
					} 
				}
				setState(1169);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,120,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCalcExprContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(VtlParser.ASSIGN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public JoinCalcExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCalcExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinCalcExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinCalcExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinCalcExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCalcExprContext joinCalcExpr() throws RecognitionException {
		JoinCalcExprContext _localctx = new JoinCalcExprContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_joinCalcExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1170);
			componentID();
			setState(1171);
			match(ASSIGN);
			setState(1172);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinAggClauseContext extends ParserRuleContext {
		public TerminalNode AGGREGATE() { return getToken(VtlParser.AGGREGATE, 0); }
		public List<JoinAggClauseItemContext> joinAggClauseItem() {
			return getRuleContexts(JoinAggClauseItemContext.class);
		}
		public JoinAggClauseItemContext joinAggClauseItem(int i) {
			return getRuleContext(JoinAggClauseItemContext.class,i);
		}
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public GroupingClauseContext groupingClause() {
			return getRuleContext(GroupingClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public JoinAggClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinAggClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinAggClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinAggClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinAggClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinAggClauseContext joinAggClause() throws RecognitionException {
		JoinAggClauseContext _localctx = new JoinAggClauseContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_joinAggClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1175);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1174);
				componentRole();
				}
			}

			setState(1177);
			match(AGGREGATE);
			setState(1178);
			joinAggClauseItem();
			setState(1183);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1179);
				match(CARTESIAN_PER);
				setState(1180);
				joinAggClauseItem();
				}
				}
				setState(1185);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1187);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(1186);
				groupingClause();
				}
			}

			setState(1190);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==HAVING) {
				{
				setState(1189);
				havingClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinAggClauseItemContext extends ParserRuleContext {
		public List<JoinAggExprContext> joinAggExpr() {
			return getRuleContexts(JoinAggExprContext.class);
		}
		public JoinAggExprContext joinAggExpr(int i) {
			return getRuleContext(JoinAggExprContext.class,i);
		}
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinAggClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinAggClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinAggClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinAggClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinAggClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinAggClauseItemContext joinAggClauseItem() throws RecognitionException {
		JoinAggClauseItemContext _localctx = new JoinAggClauseItemContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_joinAggClauseItem);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1193);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1192);
				componentRole();
				}
			}

			setState(1195);
			joinAggExpr();
			setState(1200);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,126,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1196);
					match(CARTESIAN_PER);
					setState(1197);
					joinAggExpr();
					}
					} 
				}
				setState(1202);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,126,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinAggExprContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(VtlParser.ASSIGN, 0); }
		public AggrFunctionContext aggrFunction() {
			return getRuleContext(AggrFunctionContext.class,0);
		}
		public JoinAggExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinAggExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinAggExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinAggExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinAggExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinAggExprContext joinAggExpr() throws RecognitionException {
		JoinAggExprContext _localctx = new JoinAggExprContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_joinAggExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1203);
			componentID();
			setState(1204);
			match(ASSIGN);
			setState(1205);
			aggrFunction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinKeepClauseContext extends ParserRuleContext {
		public TerminalNode KEEP() { return getToken(VtlParser.KEEP, 0); }
		public List<KeepClauseItemContext> keepClauseItem() {
			return getRuleContexts(KeepClauseItemContext.class);
		}
		public KeepClauseItemContext keepClauseItem(int i) {
			return getRuleContext(KeepClauseItemContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinKeepClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinKeepClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinKeepClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinKeepClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinKeepClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinKeepClauseContext joinKeepClause() throws RecognitionException {
		JoinKeepClauseContext _localctx = new JoinKeepClauseContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_joinKeepClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1207);
			match(KEEP);
			setState(1208);
			keepClauseItem();
			setState(1213);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1209);
				match(CARTESIAN_PER);
				setState(1210);
				keepClauseItem();
				}
				}
				setState(1215);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinDropClauseContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(VtlParser.DROP, 0); }
		public List<DropClauseItemContext> dropClauseItem() {
			return getRuleContexts(DropClauseItemContext.class);
		}
		public DropClauseItemContext dropClauseItem(int i) {
			return getRuleContext(DropClauseItemContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinDropClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinDropClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinDropClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinDropClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinDropClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinDropClauseContext joinDropClause() throws RecognitionException {
		JoinDropClauseContext _localctx = new JoinDropClauseContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_joinDropClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1216);
			match(DROP);
			setState(1217);
			dropClauseItem();
			setState(1222);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1218);
				match(CARTESIAN_PER);
				setState(1219);
				dropClauseItem();
				}
				}
				setState(1224);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinFilterClauseContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(VtlParser.FILTER, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public RulesetIDContext rulesetID() {
			return getRuleContext(RulesetIDContext.class,0);
		}
		public JoinFilterClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinFilterClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinFilterClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinFilterClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinFilterClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinFilterClauseContext joinFilterClause() throws RecognitionException {
		JoinFilterClauseContext _localctx = new JoinFilterClauseContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_joinFilterClause);
		try {
			setState(1228);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FILTER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1225);
				match(FILTER);
				setState(1226);
				expr(0);
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(1227);
				rulesetID();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRenameClauseContext extends ParserRuleContext {
		public TerminalNode RENAME() { return getToken(VtlParser.RENAME, 0); }
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public List<TerminalNode> TO() { return getTokens(VtlParser.TO); }
		public TerminalNode TO(int i) {
			return getToken(VtlParser.TO, i);
		}
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<TerminalNode> MEMBERSHIP() { return getTokens(VtlParser.MEMBERSHIP); }
		public TerminalNode MEMBERSHIP(int i) {
			return getToken(VtlParser.MEMBERSHIP, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public JoinRenameClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRenameClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinRenameClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinRenameClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinRenameClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRenameClauseContext joinRenameClause() throws RecognitionException {
		JoinRenameClauseContext _localctx = new JoinRenameClauseContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_joinRenameClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1230);
			match(RENAME);
			setState(1234);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
			case 1:
				{
				setState(1231);
				componentID();
				setState(1232);
				match(MEMBERSHIP);
				}
				break;
			}
			setState(1236);
			varID();
			setState(1237);
			match(TO);
			setState(1241);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
			case 1:
				{
				setState(1238);
				componentID();
				setState(1239);
				match(MEMBERSHIP);
				}
				break;
			}
			setState(1243);
			varID();
			setState(1261);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,134,_ctx);
			while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(1244);
					match(CARTESIAN_PER);
					setState(1248);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
					case 1:
						{
						setState(1245);
						componentID();
						setState(1246);
						match(MEMBERSHIP);
						}
						break;
					}
					setState(1250);
					varID();
					setState(1251);
					match(TO);
					setState(1255);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,133,_ctx) ) {
					case 1:
						{
						setState(1252);
						componentID();
						setState(1253);
						match(MEMBERSHIP);
						}
						break;
					}
					setState(1257);
					varID();
					}
					} 
				}
				setState(1263);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,134,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinApplyClauseContext extends ParserRuleContext {
		public TerminalNode APPLY() { return getToken(VtlParser.APPLY, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public JoinApplyClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinApplyClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinApplyClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinApplyClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinApplyClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinApplyClauseContext joinApplyClause() throws RecognitionException {
		JoinApplyClauseContext _localctx = new JoinApplyClauseContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_joinApplyClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1264);
			match(APPLY);
			setState(1265);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnFunctionContext extends ParserRuleContext {
		public TerminalNode FIRST_VALUE() { return getToken(VtlParser.FIRST_VALUE, 0); }
		public TerminalNode LAG() { return getToken(VtlParser.LAG, 0); }
		public TerminalNode LAST_VALUE() { return getToken(VtlParser.LAST_VALUE, 0); }
		public TerminalNode RANK() { return getToken(VtlParser.RANK, 0); }
		public TerminalNode RATIO_TO_REPORT() { return getToken(VtlParser.RATIO_TO_REPORT, 0); }
		public TerminalNode LEAD() { return getToken(VtlParser.LEAD, 0); }
		public AnFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anFunction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAnFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAnFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAnFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnFunctionContext anFunction() throws RecognitionException {
		AnFunctionContext _localctx = new AnFunctionContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_anFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1267);
			_la = _input.LA(1);
			if ( !(_la==RANK || ((((_la - 134)) & ~0x3f) == 0 && ((1L << (_la - 134)) & ((1L << (FIRST_VALUE - 134)) | (1L << (LAST_VALUE - 134)) | (1L << (LAG - 134)) | (1L << (LEAD - 134)) | (1L << (RATIO_TO_REPORT - 134)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregateClauseContext extends ParserRuleContext {
		public List<AggrFunctionClauseContext> aggrFunctionClause() {
			return getRuleContexts(AggrFunctionClauseContext.class);
		}
		public AggrFunctionClauseContext aggrFunctionClause(int i) {
			return getRuleContext(AggrFunctionClauseContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public AggregateClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregateClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggregateClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggregateClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggregateClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregateClauseContext aggregateClause() throws RecognitionException {
		AggregateClauseContext _localctx = new AggregateClauseContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_aggregateClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1269);
			aggrFunctionClause();
			setState(1274);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1270);
				match(CARTESIAN_PER);
				setState(1271);
				aggrFunctionClause();
				}
				}
				setState(1276);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrFunctionClauseContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(VtlParser.ASSIGN, 0); }
		public AggrFunctionContext aggrFunction() {
			return getRuleContext(AggrFunctionContext.class,0);
		}
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public AggrFunctionClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrFunctionClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrFunctionClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrFunctionClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrFunctionClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrFunctionClauseContext aggrFunctionClause() throws RecognitionException {
		AggrFunctionClauseContext _localctx = new AggrFunctionClauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_aggrFunctionClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1278);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1277);
				componentRole();
				}
			}

			setState(1280);
			componentID();
			setState(1281);
			match(ASSIGN);
			setState(1282);
			aggrFunction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GetFiltersClauseContext extends ParserRuleContext {
		public List<GetFilterClauseContext> getFilterClause() {
			return getRuleContexts(GetFilterClauseContext.class);
		}
		public GetFilterClauseContext getFilterClause(int i) {
			return getRuleContext(GetFilterClauseContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public GetFiltersClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getFiltersClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterGetFiltersClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitGetFiltersClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitGetFiltersClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GetFiltersClauseContext getFiltersClause() throws RecognitionException {
		GetFiltersClauseContext _localctx = new GetFiltersClauseContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_getFiltersClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1284);
			getFilterClause();
			setState(1289);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1285);
				match(CARTESIAN_PER);
				setState(1286);
				getFilterClause();
				}
				}
				setState(1291);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GetFilterClauseContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode FILTER() { return getToken(VtlParser.FILTER, 0); }
		public GetFilterClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getFilterClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterGetFilterClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitGetFilterClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitGetFilterClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GetFilterClauseContext getFilterClause() throws RecognitionException {
		GetFilterClauseContext _localctx = new GetFilterClauseContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_getFilterClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(1293);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FILTER) {
				{
				setState(1292);
				match(FILTER);
				}
			}

			setState(1295);
			expr(0);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrClauseContext extends ParserRuleContext {
		public TerminalNode AGGREGATE() { return getToken(VtlParser.AGGREGATE, 0); }
		public AggregateClauseContext aggregateClause() {
			return getRuleContext(AggregateClauseContext.class,0);
		}
		public GroupingClauseContext groupingClause() {
			return getRuleContext(GroupingClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public AggrClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrClauseContext aggrClause() throws RecognitionException {
		AggrClauseContext _localctx = new AggrClauseContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_aggrClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1297);
			match(AGGREGATE);
			setState(1298);
			aggregateClause();
			setState(1300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(1299);
				groupingClause();
				}
			}

			setState(1303);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==HAVING) {
				{
				setState(1302);
				havingClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FilterClauseContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(VtlParser.FILTER, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public FilterClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filterClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterFilterClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitFilterClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitFilterClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterClauseContext filterClause() throws RecognitionException {
		FilterClauseContext _localctx = new FilterClauseContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_filterClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1305);
			match(FILTER);
			setState(1306);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RenameClauseContext extends ParserRuleContext {
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public List<TerminalNode> TO() { return getTokens(VtlParser.TO); }
		public TerminalNode TO(int i) {
			return getToken(VtlParser.TO, i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public RenameClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_renameClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRenameClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRenameClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRenameClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RenameClauseContext renameClause() throws RecognitionException {
		RenameClauseContext _localctx = new RenameClauseContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_renameClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1308);
			varID();
			setState(1309);
			match(TO);
			setState(1310);
			varID();
			setState(1318);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1311);
				match(CARTESIAN_PER);
				setState(1312);
				varID();
				setState(1313);
				match(TO);
				setState(1314);
				varID();
				}
				}
				setState(1320);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrFunctionContext extends ParserRuleContext {
		public TerminalNode SUM() { return getToken(VtlParser.SUM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode AVG() { return getToken(VtlParser.AVG, 0); }
		public TerminalNode COUNT() { return getToken(VtlParser.COUNT, 0); }
		public TerminalNode MEDIAN() { return getToken(VtlParser.MEDIAN, 0); }
		public TerminalNode MIN() { return getToken(VtlParser.MIN, 0); }
		public TerminalNode MAX() { return getToken(VtlParser.MAX, 0); }
		public TerminalNode RANK() { return getToken(VtlParser.RANK, 0); }
		public TerminalNode STDDEV_POP() { return getToken(VtlParser.STDDEV_POP, 0); }
		public TerminalNode STDDEV_SAMP() { return getToken(VtlParser.STDDEV_SAMP, 0); }
		public TerminalNode VAR_POP() { return getToken(VtlParser.VAR_POP, 0); }
		public TerminalNode VAR_SAMP() { return getToken(VtlParser.VAR_SAMP, 0); }
		public AggrFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrFunction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrFunctionContext aggrFunction() throws RecognitionException {
		AggrFunctionContext _localctx = new AggrFunctionContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_aggrFunction);
		int _la;
		try {
			setState(1377);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SUM:
				enterOuterAlt(_localctx, 1);
				{
				setState(1321);
				match(SUM);
				setState(1322);
				match(T__12);
				setState(1323);
				expr(0);
				setState(1324);
				match(T__13);
				}
				break;
			case AVG:
				enterOuterAlt(_localctx, 2);
				{
				setState(1326);
				match(AVG);
				setState(1327);
				match(T__12);
				setState(1328);
				expr(0);
				setState(1329);
				match(T__13);
				}
				break;
			case COUNT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1331);
				match(COUNT);
				setState(1332);
				match(T__12);
				setState(1334);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << T__3) | (1L << T__12) | (1L << EVAL) | (1L << IF) | (1L << CURRENT_DATE) | (1L << NOT) | (1L << BETWEEN) | (1L << ISNULL) | (1L << UNION) | (1L << SYMDIFF) | (1L << INTERSECT) | (1L << CHECK) | (1L << EXISTS_IN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (RANK - 64)) | (1L << (MIN - 64)) | (1L << (MAX - 64)) | (1L << (ABS - 64)) | (1L << (LN - 64)) | (1L << (LOG - 64)) | (1L << (TRUNC - 64)) | (1L << (ROUND - 64)) | (1L << (POWER - 64)) | (1L << (MOD - 64)) | (1L << (LEN - 64)) | (1L << (TRIM - 64)) | (1L << (UCASE - 64)) | (1L << (LCASE - 64)) | (1L << (SUBSTR - 64)) | (1L << (SUM - 64)) | (1L << (AVG - 64)) | (1L << (MEDIAN - 64)) | (1L << (COUNT - 64)) | (1L << (EXP - 64)) | (1L << (CHARSET_MATCH - 64)) | (1L << (NVL - 64)) | (1L << (HIERARCHY - 64)) | (1L << (DEFINE - 64)) | (1L << (LTRIM - 64)) | (1L << (RTRIM - 64)) | (1L << (INSTR - 64)) | (1L << (REPLACE - 64)) | (1L << (CEIL - 64)) | (1L << (FLOOR - 64)) | (1L << (SQRT - 64)) | (1L << (SETDIFF - 64)) | (1L << (STDDEV_POP - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (STDDEV_SAMP - 128)) | (1L << (VAR_POP - 128)) | (1L << (VAR_SAMP - 128)) | (1L << (FIRST_VALUE - 128)) | (1L << (LAST_VALUE - 128)) | (1L << (LAG - 128)) | (1L << (LEAD - 128)) | (1L << (RATIO_TO_REPORT - 128)) | (1L << (FILL_TIME_SERIES - 128)) | (1L << (FLOW_TO_STOCK - 128)) | (1L << (STOCK_TO_FLOW - 128)) | (1L << (TIMESHIFT - 128)) | (1L << (INNER_JOIN - 128)) | (1L << (LEFT_JOIN - 128)) | (1L << (CROSS_JOIN - 128)) | (1L << (FULL_JOIN - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PERIOD_INDICATOR - 193)) | (1L << (TIME_AGG - 193)) | (1L << (CAST - 193)) | (1L << (CHECK_DATAPOINT - 193)) | (1L << (CHECK_HIERARCHY - 193)) | (1L << (INTEGER_CONSTANT - 193)) | (1L << (FLOAT_CONSTANT - 193)) | (1L << (BOOLEAN_CONSTANT - 193)) | (1L << (NULL_CONSTANT - 193)) | (1L << (STRING_CONSTANT - 193)) | (1L << (IDENTIFIER - 193)))) != 0)) {
					{
					setState(1333);
					expr(0);
					}
				}

				setState(1336);
				match(T__13);
				}
				break;
			case MEDIAN:
				enterOuterAlt(_localctx, 4);
				{
				setState(1337);
				match(MEDIAN);
				setState(1338);
				match(T__12);
				setState(1339);
				expr(0);
				setState(1340);
				match(T__13);
				}
				break;
			case MIN:
				enterOuterAlt(_localctx, 5);
				{
				setState(1342);
				match(MIN);
				setState(1343);
				match(T__12);
				setState(1344);
				expr(0);
				setState(1345);
				match(T__13);
				}
				break;
			case MAX:
				enterOuterAlt(_localctx, 6);
				{
				setState(1347);
				match(MAX);
				setState(1348);
				match(T__12);
				setState(1349);
				expr(0);
				setState(1350);
				match(T__13);
				}
				break;
			case RANK:
				enterOuterAlt(_localctx, 7);
				{
				setState(1352);
				match(RANK);
				setState(1353);
				match(T__12);
				setState(1354);
				expr(0);
				setState(1355);
				match(T__13);
				}
				break;
			case STDDEV_POP:
				enterOuterAlt(_localctx, 8);
				{
				setState(1357);
				match(STDDEV_POP);
				setState(1358);
				match(T__12);
				setState(1359);
				expr(0);
				setState(1360);
				match(T__13);
				}
				break;
			case STDDEV_SAMP:
				enterOuterAlt(_localctx, 9);
				{
				setState(1362);
				match(STDDEV_SAMP);
				setState(1363);
				match(T__12);
				setState(1364);
				expr(0);
				setState(1365);
				match(T__13);
				}
				break;
			case VAR_POP:
				enterOuterAlt(_localctx, 10);
				{
				setState(1367);
				match(VAR_POP);
				setState(1368);
				match(T__12);
				setState(1369);
				expr(0);
				setState(1370);
				match(T__13);
				}
				break;
			case VAR_SAMP:
				enterOuterAlt(_localctx, 11);
				{
				setState(1372);
				match(VAR_SAMP);
				setState(1373);
				match(T__12);
				setState(1374);
				expr(0);
				setState(1375);
				match(T__13);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CalcClauseContext extends ParserRuleContext {
		public TerminalNode CALC() { return getToken(VtlParser.CALC, 0); }
		public List<CalcClauseItemContext> calcClauseItem() {
			return getRuleContexts(CalcClauseItemContext.class);
		}
		public CalcClauseItemContext calcClauseItem(int i) {
			return getRuleContext(CalcClauseItemContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public CalcClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_calcClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCalcClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCalcClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCalcClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CalcClauseContext calcClause() throws RecognitionException {
		CalcClauseContext _localctx = new CalcClauseContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_calcClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1379);
			match(CALC);
			setState(1380);
			calcClauseItem();
			setState(1385);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1381);
				match(CARTESIAN_PER);
				setState(1382);
				calcClauseItem();
				}
				}
				setState(1387);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CalcClauseItemContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public TerminalNode ASSIGN() { return getToken(VtlParser.ASSIGN, 0); }
		public CalcExprContext calcExpr() {
			return getRuleContext(CalcExprContext.class,0);
		}
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public CalcClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_calcClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCalcClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCalcClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCalcClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CalcClauseItemContext calcClauseItem() throws RecognitionException {
		CalcClauseItemContext _localctx = new CalcClauseItemContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_calcClauseItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1389);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 90)) & ~0x3f) == 0 && ((1L << (_la - 90)) & ((1L << (DIMENSION - 90)) | (1L << (MEASURE - 90)) | (1L << (ATTRIBUTE - 90)) | (1L << (VIRAL - 90)))) != 0) || _la==COMPONENT) {
				{
				setState(1388);
				componentRole();
				}
			}

			setState(1391);
			componentID();
			setState(1392);
			match(ASSIGN);
			setState(1393);
			calcExpr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CalcExprContext extends ParserRuleContext {
		public AggrFunctionContext aggrFunction() {
			return getRuleContext(AggrFunctionContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public CalcExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_calcExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCalcExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCalcExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCalcExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CalcExprContext calcExpr() throws RecognitionException {
		CalcExprContext _localctx = new CalcExprContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_calcExpr);
		try {
			setState(1401);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,146,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1395);
				aggrFunction();
				setState(1396);
				match(T__12);
				setState(1397);
				expr(0);
				setState(1398);
				match(T__13);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1400);
				expr(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropClauseContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(VtlParser.DROP, 0); }
		public List<DropClauseItemContext> dropClauseItem() {
			return getRuleContexts(DropClauseItemContext.class);
		}
		public DropClauseItemContext dropClauseItem(int i) {
			return getRuleContext(DropClauseItemContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public DropClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDropClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDropClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDropClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropClauseContext dropClause() throws RecognitionException {
		DropClauseContext _localctx = new DropClauseContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_dropClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1403);
			match(DROP);
			setState(1404);
			dropClauseItem();
			setState(1409);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1405);
				match(CARTESIAN_PER);
				setState(1406);
				dropClauseItem();
				}
				}
				setState(1411);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropClauseItemContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public DatasetIDContext datasetID() {
			return getRuleContext(DatasetIDContext.class,0);
		}
		public TerminalNode MEMBERSHIP() { return getToken(VtlParser.MEMBERSHIP, 0); }
		public DropClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDropClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDropClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDropClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropClauseItemContext dropClauseItem() throws RecognitionException {
		DropClauseItemContext _localctx = new DropClauseItemContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_dropClauseItem);
		try {
			setState(1417);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,148,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1412);
				componentID();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1413);
				datasetID();
				setState(1414);
				match(MEMBERSHIP);
				setState(1415);
				componentID();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeepClauseContext extends ParserRuleContext {
		public TerminalNode KEEP() { return getToken(VtlParser.KEEP, 0); }
		public List<KeepClauseItemContext> keepClauseItem() {
			return getRuleContexts(KeepClauseItemContext.class);
		}
		public KeepClauseItemContext keepClauseItem(int i) {
			return getRuleContext(KeepClauseItemContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public KeepClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keepClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterKeepClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitKeepClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitKeepClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeepClauseContext keepClause() throws RecognitionException {
		KeepClauseContext _localctx = new KeepClauseContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_keepClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1419);
			match(KEEP);
			setState(1420);
			keepClauseItem();
			setState(1425);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1421);
				match(CARTESIAN_PER);
				setState(1422);
				keepClauseItem();
				}
				}
				setState(1427);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeepClauseItemContext extends ParserRuleContext {
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public DatasetIDContext datasetID() {
			return getRuleContext(DatasetIDContext.class,0);
		}
		public TerminalNode MEMBERSHIP() { return getToken(VtlParser.MEMBERSHIP, 0); }
		public KeepClauseItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keepClauseItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterKeepClauseItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitKeepClauseItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitKeepClauseItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeepClauseItemContext keepClauseItem() throws RecognitionException {
		KeepClauseItemContext _localctx = new KeepClauseItemContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_keepClauseItem);
		try {
			setState(1433);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,150,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1428);
				componentID();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1429);
				datasetID();
				setState(1430);
				match(MEMBERSHIP);
				setState(1431);
				componentID();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnpivotExprContext extends ParserRuleContext {
		public TerminalNode UNPIVOT() { return getToken(VtlParser.UNPIVOT, 0); }
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public UnpivotExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unpivotExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterUnpivotExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitUnpivotExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitUnpivotExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnpivotExprContext unpivotExpr() throws RecognitionException {
		UnpivotExprContext _localctx = new UnpivotExprContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_unpivotExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1435);
			match(UNPIVOT);
			setState(1436);
			varID();
			setState(1437);
			match(CARTESIAN_PER);
			setState(1438);
			varID();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotExprContext extends ParserRuleContext {
		public TerminalNode PIVOT() { return getToken(VtlParser.PIVOT, 0); }
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public PivotExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPivotExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPivotExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPivotExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotExprContext pivotExpr() throws RecognitionException {
		PivotExprContext _localctx = new PivotExprContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_pivotExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1440);
			match(PIVOT);
			setState(1441);
			varID();
			setState(1442);
			match(CARTESIAN_PER);
			setState(1443);
			varID();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubspaceExprContext extends ParserRuleContext {
		public TerminalNode SUBSPACE() { return getToken(VtlParser.SUBSPACE, 0); }
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public SubspaceExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subspaceExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSubspaceExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSubspaceExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSubspaceExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubspaceExprContext subspaceExpr() throws RecognitionException {
		SubspaceExprContext _localctx = new SubspaceExprContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_subspaceExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1445);
			match(SUBSPACE);
			setState(1446);
			varID();
			setState(1447);
			match(T__10);
			setState(1448);
			constant();
			setState(1456);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1449);
				match(CARTESIAN_PER);
				setState(1450);
				varID();
				setState(1451);
				match(T__10);
				setState(1452);
				constant();
				}
				}
				setState(1458);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InBetweenClauseContext extends ParserRuleContext {
		public TerminalNode IN() { return getToken(VtlParser.IN, 0); }
		public SetExprContext setExpr() {
			return getRuleContext(SetExprContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public TerminalNode NOT_IN() { return getToken(VtlParser.NOT_IN, 0); }
		public InBetweenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inBetweenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterInBetweenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitInBetweenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitInBetweenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InBetweenClauseContext inBetweenClause() throws RecognitionException {
		InBetweenClauseContext _localctx = new InBetweenClauseContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_inBetweenClause);
		try {
			setState(1469);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1459);
				match(IN);
				setState(1462);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case UNION:
				case SYMDIFF:
				case INTERSECT:
				case SETDIFF:
					{
					setState(1460);
					setExpr();
					}
					break;
				case IDENTIFIER:
					{
					setState(1461);
					match(IDENTIFIER);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case NOT_IN:
				enterOuterAlt(_localctx, 2);
				{
				setState(1464);
				match(NOT_IN);
				setState(1467);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case UNION:
				case SYMDIFF:
				case INTERSECT:
				case SETDIFF:
					{
					setState(1465);
					setExpr();
					}
					break;
				case IDENTIFIER:
					{
					setState(1466);
					match(IDENTIFIER);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetExprContext extends ParserRuleContext {
		public TerminalNode UNION() { return getToken(VtlParser.UNION, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public TerminalNode SYMDIFF() { return getToken(VtlParser.SYMDIFF, 0); }
		public TerminalNode SETDIFF() { return getToken(VtlParser.SETDIFF, 0); }
		public TerminalNode INTERSECT() { return getToken(VtlParser.INTERSECT, 0); }
		public SetExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSetExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSetExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSetExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetExprContext setExpr() throws RecognitionException {
		SetExprContext _localctx = new SetExprContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_setExpr);
		int _la;
		try {
			setState(1509);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case UNION:
				enterOuterAlt(_localctx, 1);
				{
				setState(1471);
				match(UNION);
				setState(1472);
				match(T__12);
				setState(1473);
				expr(0);
				setState(1478);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(1474);
					match(CARTESIAN_PER);
					setState(1475);
					expr(0);
					}
					}
					setState(1480);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1481);
				match(T__13);
				}
				break;
			case SYMDIFF:
				enterOuterAlt(_localctx, 2);
				{
				setState(1483);
				match(SYMDIFF);
				setState(1484);
				match(T__12);
				setState(1485);
				expr(0);
				setState(1486);
				match(CARTESIAN_PER);
				setState(1487);
				expr(0);
				setState(1488);
				match(T__13);
				}
				break;
			case SETDIFF:
				enterOuterAlt(_localctx, 3);
				{
				setState(1490);
				match(SETDIFF);
				setState(1491);
				match(T__12);
				setState(1492);
				expr(0);
				setState(1493);
				match(CARTESIAN_PER);
				setState(1494);
				expr(0);
				setState(1495);
				match(T__13);
				}
				break;
			case INTERSECT:
				enterOuterAlt(_localctx, 4);
				{
				setState(1497);
				match(INTERSECT);
				setState(1498);
				match(T__12);
				setState(1499);
				expr(0);
				setState(1504);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(1500);
					match(CARTESIAN_PER);
					setState(1501);
					expr(0);
					}
					}
					setState(1506);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1507);
				match(T__13);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubscriptExprContext extends ParserRuleContext {
		public PersistentDatasetIDContext persistentDatasetID() {
			return getRuleContext(PersistentDatasetIDContext.class,0);
		}
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public TerminalNode CARTESIAN_PER() { return getToken(VtlParser.CARTESIAN_PER, 0); }
		public SubscriptExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subscriptExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSubscriptExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSubscriptExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSubscriptExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubscriptExprContext subscriptExpr() throws RecognitionException {
		SubscriptExprContext _localctx = new SubscriptExprContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_subscriptExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1511);
			persistentDatasetID();
			setState(1512);
			match(T__0);
			setState(1513);
			componentID();
			setState(1514);
			match(T__10);
			setState(1515);
			constant();
			setState(1521);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CARTESIAN_PER) {
				{
				setState(1516);
				match(CARTESIAN_PER);
				setState(1517);
				componentID();
				setState(1518);
				match(T__10);
				setState(1519);
				constant();
				}
			}

			setState(1523);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrInvocationContext extends ParserRuleContext {
		public AggrFunctionNameContext aggrFunctionName() {
			return getRuleContext(AggrFunctionNameContext.class,0);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> MEMBERSHIP() { return getTokens(VtlParser.MEMBERSHIP); }
		public TerminalNode MEMBERSHIP(int i) {
			return getToken(VtlParser.MEMBERSHIP, i);
		}
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public GroupingClauseContext groupingClause() {
			return getRuleContext(GroupingClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public AggrInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrInvocationContext aggrInvocation() throws RecognitionException {
		AggrInvocationContext _localctx = new AggrInvocationContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_aggrInvocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1525);
			aggrFunctionName();
			setState(1526);
			match(T__12);
			setState(1527);
			match(IDENTIFIER);
			setState(1530);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MEMBERSHIP) {
				{
				setState(1528);
				match(MEMBERSHIP);
				setState(1529);
				componentID();
				}
			}

			setState(1540);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1532);
				match(CARTESIAN_PER);
				setState(1533);
				match(IDENTIFIER);
				setState(1536);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MEMBERSHIP) {
					{
					setState(1534);
					match(MEMBERSHIP);
					setState(1535);
					componentID();
					}
				}

				}
				}
				setState(1542);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1544);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(1543);
				groupingClause();
				}
			}

			setState(1547);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==HAVING) {
				{
				setState(1546);
				havingClause();
				}
			}

			setState(1549);
			match(T__13);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrInvocationCompExprContext extends ParserRuleContext {
		public AggrFunctionNameContext aggrFunctionName() {
			return getRuleContext(AggrFunctionNameContext.class,0);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public List<TerminalNode> MEMBERSHIP() { return getTokens(VtlParser.MEMBERSHIP); }
		public TerminalNode MEMBERSHIP(int i) {
			return getToken(VtlParser.MEMBERSHIP, i);
		}
		public List<ComponentIDContext> componentID() {
			return getRuleContexts(ComponentIDContext.class);
		}
		public ComponentIDContext componentID(int i) {
			return getRuleContext(ComponentIDContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public GroupingClauseContext groupingClause() {
			return getRuleContext(GroupingClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public AggrInvocationCompExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrInvocationCompExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrInvocationCompExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrInvocationCompExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrInvocationCompExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrInvocationCompExprContext aggrInvocationCompExpr() throws RecognitionException {
		AggrInvocationCompExprContext _localctx = new AggrInvocationCompExprContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_aggrInvocationCompExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1551);
			aggrFunctionName();
			setState(1552);
			match(T__12);
			setState(1553);
			match(IDENTIFIER);
			setState(1556);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MEMBERSHIP) {
				{
				setState(1554);
				match(MEMBERSHIP);
				setState(1555);
				componentID();
				}
			}

			setState(1566);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CARTESIAN_PER) {
				{
				{
				setState(1558);
				match(CARTESIAN_PER);
				setState(1559);
				match(IDENTIFIER);
				setState(1562);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MEMBERSHIP) {
					{
					setState(1560);
					match(MEMBERSHIP);
					setState(1561);
					componentID();
					}
				}

				}
				}
				setState(1568);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1569);
			match(T__13);
			setState(1571);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
			case 1:
				{
				setState(1570);
				groupingClause();
				}
				break;
			}
			setState(1574);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
			case 1:
				{
				setState(1573);
				havingClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggrFunctionNameContext extends ParserRuleContext {
		public TerminalNode SUM() { return getToken(VtlParser.SUM, 0); }
		public TerminalNode AVG() { return getToken(VtlParser.AVG, 0); }
		public TerminalNode COUNT() { return getToken(VtlParser.COUNT, 0); }
		public TerminalNode MEDIAN() { return getToken(VtlParser.MEDIAN, 0); }
		public TerminalNode MIN() { return getToken(VtlParser.MIN, 0); }
		public TerminalNode MAX() { return getToken(VtlParser.MAX, 0); }
		public TerminalNode STDDEV_POP() { return getToken(VtlParser.STDDEV_POP, 0); }
		public TerminalNode STDDEV_SAMP() { return getToken(VtlParser.STDDEV_SAMP, 0); }
		public TerminalNode VAR_POP() { return getToken(VtlParser.VAR_POP, 0); }
		public TerminalNode VAR_SAMP() { return getToken(VtlParser.VAR_SAMP, 0); }
		public AggrFunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggrFunctionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterAggrFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitAggrFunctionName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitAggrFunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggrFunctionNameContext aggrFunctionName() throws RecognitionException {
		AggrFunctionNameContext _localctx = new AggrFunctionNameContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_aggrFunctionName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1576);
			_la = _input.LA(1);
			if ( !(((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (MIN - 67)) | (1L << (MAX - 67)) | (1L << (SUM - 67)) | (1L << (AVG - 67)) | (1L << (MEDIAN - 67)) | (1L << (COUNT - 67)) | (1L << (STDDEV_POP - 67)) | (1L << (STDDEV_SAMP - 67)) | (1L << (VAR_POP - 67)) | (1L << (VAR_SAMP - 67)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingClauseContext extends ParserRuleContext {
		public GroupKeywordContext groupKeyword() {
			return getRuleContext(GroupKeywordContext.class,0);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public GroupingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterGroupingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitGroupingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitGroupingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingClauseContext groupingClause() throws RecognitionException {
		GroupingClauseContext _localctx = new GroupingClauseContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_groupingClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1578);
			groupKeyword();
			setState(1588);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
			case 1:
				{
				{
				setState(1579);
				match(IDENTIFIER);
				setState(1584);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,169,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1580);
						match(CARTESIAN_PER);
						setState(1581);
						match(IDENTIFIER);
						}
						} 
					}
					setState(1586);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,169,_ctx);
				}
				}
				}
				break;
			case 2:
				{
				{
				setState(1587);
				expr(0);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(VtlParser.HAVING, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public AggrFunctionContext aggrFunction() {
			return getRuleContext(AggrFunctionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitHavingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1590);
			match(HAVING);
			setState(1592);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
			case 1:
				{
				setState(1591);
				match(T__12);
				}
				break;
			}
			setState(1595);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
			case 1:
				{
				setState(1594);
				aggrFunction();
				}
				break;
			}
			setState(1597);
			expr(0);
			setState(1599);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
			case 1:
				{
				setState(1598);
				match(T__13);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ReturnAllContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(VtlParser.RETURN, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public TerminalNode DATA() { return getToken(VtlParser.DATA, 0); }
		public TerminalNode POINTS() { return getToken(VtlParser.POINTS, 0); }
		public ReturnAllContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_returnAll; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterReturnAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitReturnAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitReturnAll(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ReturnAllContext returnAll() throws RecognitionException {
		ReturnAllContext _localctx = new ReturnAllContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_returnAll);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1601);
			match(RETURN);
			setState(1602);
			match(ALL);
			setState(1603);
			match(DATA);
			setState(1604);
			match(POINTS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComponentRoleContext extends ParserRuleContext {
		public TerminalNode MEASURE() { return getToken(VtlParser.MEASURE, 0); }
		public TerminalNode COMPONENT() { return getToken(VtlParser.COMPONENT, 0); }
		public TerminalNode DIMENSION() { return getToken(VtlParser.DIMENSION, 0); }
		public TerminalNode ATTRIBUTE() { return getToken(VtlParser.ATTRIBUTE, 0); }
		public ViralAttributeContext viralAttribute() {
			return getRuleContext(ViralAttributeContext.class,0);
		}
		public ComponentRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_componentRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterComponentRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitComponentRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitComponentRole(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComponentRoleContext componentRole() throws RecognitionException {
		ComponentRoleContext _localctx = new ComponentRoleContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_componentRole);
		try {
			setState(1611);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MEASURE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1606);
				match(MEASURE);
				}
				break;
			case COMPONENT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1607);
				match(COMPONENT);
				}
				break;
			case DIMENSION:
				enterOuterAlt(_localctx, 3);
				{
				setState(1608);
				match(DIMENSION);
				}
				break;
			case ATTRIBUTE:
				enterOuterAlt(_localctx, 4);
				{
				setState(1609);
				match(ATTRIBUTE);
				}
				break;
			case VIRAL:
				enterOuterAlt(_localctx, 5);
				{
				setState(1610);
				viralAttribute();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ViralAttributeContext extends ParserRuleContext {
		public TerminalNode VIRAL() { return getToken(VtlParser.VIRAL, 0); }
		public TerminalNode ATTRIBUTE() { return getToken(VtlParser.ATTRIBUTE, 0); }
		public ViralAttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viralAttribute; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterViralAttribute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitViralAttribute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitViralAttribute(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ViralAttributeContext viralAttribute() throws RecognitionException {
		ViralAttributeContext _localctx = new ViralAttributeContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_viralAttribute);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1613);
			match(VIRAL);
			setState(1614);
			match(ATTRIBUTE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogBaseContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public LogBaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logBase; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterLogBase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitLogBase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitLogBase(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogBaseContext logBase() throws RecognitionException {
		LogBaseContext _localctx = new LogBaseContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_logBase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1616);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExponentContext extends ParserRuleContext {
		public TerminalNode INTEGER_CONSTANT() { return getToken(VtlParser.INTEGER_CONSTANT, 0); }
		public TerminalNode FLOAT_CONSTANT() { return getToken(VtlParser.FLOAT_CONSTANT, 0); }
		public ExponentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exponent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterExponent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitExponent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitExponent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExponentContext exponent() throws RecognitionException {
		ExponentContext _localctx = new ExponentContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_exponent);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1618);
			_la = _input.LA(1);
			if ( !(_la==INTEGER_CONSTANT || _la==FLOAT_CONSTANT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PersistentDatasetIDContext extends ParserRuleContext {
		public TerminalNode STRING_CONSTANT() { return getToken(VtlParser.STRING_CONSTANT, 0); }
		public PersistentDatasetIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_persistentDatasetID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterPersistentDatasetID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitPersistentDatasetID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitPersistentDatasetID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PersistentDatasetIDContext persistentDatasetID() throws RecognitionException {
		PersistentDatasetIDContext _localctx = new PersistentDatasetIDContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_persistentDatasetID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1620);
			match(STRING_CONSTANT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasetIDContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public DatasetIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasetID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDatasetID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDatasetID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDatasetID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasetIDContext datasetID() throws RecognitionException {
		DatasetIDContext _localctx = new DatasetIDContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_datasetID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1622);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RulesetIDContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public RulesetIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rulesetID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRulesetID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRulesetID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRulesetID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RulesetIDContext rulesetID() throws RecognitionException {
		RulesetIDContext _localctx = new RulesetIDContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_rulesetID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1624);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VarIDContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public VarIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterVarID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitVarID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitVarID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VarIDContext varID() throws RecognitionException {
		VarIDContext _localctx = new VarIDContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_varID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1626);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComponentIDContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public ComponentIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_componentID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterComponentID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitComponentID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitComponentID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComponentIDContext componentID() throws RecognitionException {
		ComponentIDContext _localctx = new ComponentIDContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_componentID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1628);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OperatorIDContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public OperatorIDContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operatorID; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterOperatorID(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitOperatorID(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitOperatorID(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OperatorIDContext operatorID() throws RecognitionException {
		OperatorIDContext _localctx = new OperatorIDContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_operatorID);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1630);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RoutineNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public RoutineNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routineName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRoutineName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRoutineName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRoutineName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RoutineNameContext routineName() throws RecognitionException {
		RoutineNameContext _localctx = new RoutineNameContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_routineName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1632);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinKeywordContext extends ParserRuleContext {
		public TerminalNode INNER_JOIN() { return getToken(VtlParser.INNER_JOIN, 0); }
		public TerminalNode LEFT_JOIN() { return getToken(VtlParser.LEFT_JOIN, 0); }
		public TerminalNode FULL_JOIN() { return getToken(VtlParser.FULL_JOIN, 0); }
		public TerminalNode CROSS_JOIN() { return getToken(VtlParser.CROSS_JOIN, 0); }
		public JoinKeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinKeyword; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterJoinKeyword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitJoinKeyword(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitJoinKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinKeywordContext joinKeyword() throws RecognitionException {
		JoinKeywordContext _localctx = new JoinKeywordContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_joinKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1634);
			_la = _input.LA(1);
			if ( !(((((_la - 179)) & ~0x3f) == 0 && ((1L << (_la - 179)) & ((1L << (INNER_JOIN - 179)) | (1L << (LEFT_JOIN - 179)) | (1L << (CROSS_JOIN - 179)) | (1L << (FULL_JOIN - 179)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupKeywordContext extends ParserRuleContext {
		public TerminalNode GROUP() { return getToken(VtlParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(VtlParser.BY, 0); }
		public TerminalNode EXCEPT() { return getToken(VtlParser.EXCEPT, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public GroupKeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupKeyword; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterGroupKeyword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitGroupKeyword(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitGroupKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupKeywordContext groupKeyword() throws RecognitionException {
		GroupKeywordContext _localctx = new GroupKeywordContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_groupKeyword);
		try {
			setState(1642);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1636);
				match(GROUP);
				setState(1637);
				match(BY);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1638);
				match(GROUP);
				setState(1639);
				match(EXCEPT);
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1640);
				match(GROUP);
				setState(1641);
				match(ALL);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public TerminalNode INTEGER_CONSTANT() { return getToken(VtlParser.INTEGER_CONSTANT, 0); }
		public TerminalNode FLOAT_CONSTANT() { return getToken(VtlParser.FLOAT_CONSTANT, 0); }
		public TerminalNode BOOLEAN_CONSTANT() { return getToken(VtlParser.BOOLEAN_CONSTANT, 0); }
		public TerminalNode STRING_CONSTANT() { return getToken(VtlParser.STRING_CONSTANT, 0); }
		public TerminalNode NULL_CONSTANT() { return getToken(VtlParser.NULL_CONSTANT, 0); }
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterConstant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitConstant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitConstant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_constant);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1644);
			_la = _input.LA(1);
			if ( !(((((_la - 226)) & ~0x3f) == 0 && ((1L << (_la - 226)) & ((1L << (INTEGER_CONSTANT - 226)) | (1L << (FLOAT_CONSTANT - 226)) | (1L << (BOOLEAN_CONSTANT - 226)) | (1L << (NULL_CONSTANT - 226)) | (1L << (STRING_CONSTANT - 226)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComponentType2Context extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(VtlParser.STRING, 0); }
		public TerminalNode INTEGER() { return getToken(VtlParser.INTEGER, 0); }
		public TerminalNode FLOAT() { return getToken(VtlParser.FLOAT, 0); }
		public TerminalNode BOOLEAN() { return getToken(VtlParser.BOOLEAN, 0); }
		public TerminalNode DATE() { return getToken(VtlParser.DATE, 0); }
		public ComponentType2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_componentType2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterComponentType2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitComponentType2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitComponentType2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComponentType2Context componentType2() throws RecognitionException {
		ComponentType2Context _localctx = new ComponentType2Context(_ctx, getState());
		enterRule(_localctx, 204, RULE_componentType2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1646);
			_la = _input.LA(1);
			if ( !(((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & ((1L << (BOOLEAN - 155)) | (1L << (DATE - 155)) | (1L << (STRING - 155)) | (1L << (INTEGER - 155)) | (1L << (FLOAT - 155)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScalarTypeContext extends ParserRuleContext {
		public BasicScalarTypeContext basicScalarType() {
			return getRuleContext(BasicScalarTypeContext.class,0);
		}
		public ValueDomainNameContext valueDomainName() {
			return getRuleContext(ValueDomainNameContext.class,0);
		}
		public SetNameContext setName() {
			return getRuleContext(SetNameContext.class,0);
		}
		public ScalarTypeConstraintContext scalarTypeConstraint() {
			return getRuleContext(ScalarTypeConstraintContext.class,0);
		}
		public TerminalNode NULL_CONSTANT() { return getToken(VtlParser.NULL_CONSTANT, 0); }
		public TerminalNode NOT() { return getToken(VtlParser.NOT, 0); }
		public ScalarTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scalarType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterScalarType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitScalarType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitScalarType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScalarTypeContext scalarType() throws RecognitionException {
		ScalarTypeContext _localctx = new ScalarTypeContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_scalarType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1651);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
			case 1:
				{
				setState(1648);
				basicScalarType();
				}
				break;
			case 2:
				{
				setState(1649);
				valueDomainName();
				}
				break;
			case 3:
				{
				setState(1650);
				setName();
				}
				break;
			}
			setState(1654);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0 || _la==T__15) {
				{
				setState(1653);
				scalarTypeConstraint();
				}
			}

			setState(1660);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT || _la==NULL_CONSTANT) {
				{
				setState(1657);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1656);
					match(NOT);
					}
				}

				setState(1659);
				match(NULL_CONSTANT);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BasicScalarTypeContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(VtlParser.STRING, 0); }
		public TerminalNode INTEGER() { return getToken(VtlParser.INTEGER, 0); }
		public TerminalNode NUMBER() { return getToken(VtlParser.NUMBER, 0); }
		public TerminalNode BOOLEAN() { return getToken(VtlParser.BOOLEAN, 0); }
		public TerminalNode DATE() { return getToken(VtlParser.DATE, 0); }
		public TerminalNode TIME_PERIOD() { return getToken(VtlParser.TIME_PERIOD, 0); }
		public TerminalNode DURATION() { return getToken(VtlParser.DURATION, 0); }
		public TerminalNode SCALAR() { return getToken(VtlParser.SCALAR, 0); }
		public TerminalNode TIME() { return getToken(VtlParser.TIME, 0); }
		public BasicScalarTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basicScalarType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterBasicScalarType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitBasicScalarType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitBasicScalarType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasicScalarTypeContext basicScalarType() throws RecognitionException {
		BasicScalarTypeContext _localctx = new BasicScalarTypeContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_basicScalarType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1662);
			_la = _input.LA(1);
			if ( !(((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & ((1L << (BOOLEAN - 155)) | (1L << (DATE - 155)) | (1L << (TIME_PERIOD - 155)) | (1L << (NUMBER - 155)) | (1L << (STRING - 155)) | (1L << (INTEGER - 155)) | (1L << (DURATION - 155)) | (1L << (SCALAR - 155)))) != 0) || _la==TIME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueDomainNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public ValueDomainNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueDomainName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterValueDomainName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitValueDomainName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitValueDomainName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueDomainNameContext valueDomainName() throws RecognitionException {
		ValueDomainNameContext _localctx = new ValueDomainNameContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_valueDomainName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1664);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public SetNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterSetName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitSetName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitSetName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetNameContext setName() throws RecognitionException {
		SetNameContext _localctx = new SetNameContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_setName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1666);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScalarTypeConstraintContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public ScalarTypeConstraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scalarTypeConstraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterScalarTypeConstraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitScalarTypeConstraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitScalarTypeConstraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScalarTypeConstraintContext scalarTypeConstraint() throws RecognitionException {
		ScalarTypeConstraintContext _localctx = new ScalarTypeConstraintContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_scalarTypeConstraint);
		int _la;
		try {
			setState(1683);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1668);
				match(T__0);
				setState(1669);
				expr(0);
				setState(1670);
				match(T__1);
				}
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1672);
				match(T__15);
				setState(1673);
				constant();
				setState(1678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(1674);
					match(CARTESIAN_PER);
					setState(1675);
					constant();
					}
					}
					setState(1680);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1681);
				match(T__16);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public ScalarTypeContext scalarType() {
			return getRuleContext(ScalarTypeContext.class,0);
		}
		public ComponentTypeContext componentType() {
			return getRuleContext(ComponentTypeContext.class,0);
		}
		public DatasetTypeContext datasetType() {
			return getRuleContext(DatasetTypeContext.class,0);
		}
		public ScalarSetTypeContext scalarSetType() {
			return getRuleContext(ScalarSetTypeContext.class,0);
		}
		public OperatorTypeContext operatorType() {
			return getRuleContext(OperatorTypeContext.class,0);
		}
		public RulesetTypeContext rulesetType() {
			return getRuleContext(RulesetTypeContext.class,0);
		}
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_dataType);
		try {
			setState(1691);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,182,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1685);
				scalarType();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1686);
				componentType();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1687);
				datasetType();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1688);
				scalarSetType();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1689);
				operatorType();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1690);
				rulesetType();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComponentTypeContext extends ParserRuleContext {
		public ComponentRoleContext componentRole() {
			return getRuleContext(ComponentRoleContext.class,0);
		}
		public ScalarTypeContext scalarType() {
			return getRuleContext(ScalarTypeContext.class,0);
		}
		public ComponentTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_componentType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterComponentType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitComponentType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitComponentType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComponentTypeContext componentType() throws RecognitionException {
		ComponentTypeContext _localctx = new ComponentTypeContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_componentType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1693);
			componentRole();
			setState(1698);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__7) {
				{
				setState(1694);
				match(T__7);
				setState(1695);
				scalarType();
				setState(1696);
				match(T__6);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasetTypeContext extends ParserRuleContext {
		public TerminalNode DATASET() { return getToken(VtlParser.DATASET, 0); }
		public List<CompConstraintContext> compConstraint() {
			return getRuleContexts(CompConstraintContext.class);
		}
		public CompConstraintContext compConstraint(int i) {
			return getRuleContext(CompConstraintContext.class,i);
		}
		public List<TerminalNode> CARTESIAN_PER() { return getTokens(VtlParser.CARTESIAN_PER); }
		public TerminalNode CARTESIAN_PER(int i) {
			return getToken(VtlParser.CARTESIAN_PER, i);
		}
		public DatasetTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasetType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDatasetType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDatasetType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDatasetType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasetTypeContext datasetType() throws RecognitionException {
		DatasetTypeContext _localctx = new DatasetTypeContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_datasetType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1700);
			match(DATASET);
			setState(1712);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__15) {
				{
				setState(1701);
				match(T__15);
				setState(1702);
				compConstraint();
				setState(1707);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CARTESIAN_PER) {
					{
					{
					setState(1703);
					match(CARTESIAN_PER);
					setState(1704);
					compConstraint();
					}
					}
					setState(1709);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1710);
				match(T__16);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CompConstraintContext extends ParserRuleContext {
		public ComponentTypeContext componentType() {
			return getRuleContext(ComponentTypeContext.class,0);
		}
		public ComponentIDContext componentID() {
			return getRuleContext(ComponentIDContext.class,0);
		}
		public MultModifierContext multModifier() {
			return getRuleContext(MultModifierContext.class,0);
		}
		public CompConstraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compConstraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterCompConstraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitCompConstraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitCompConstraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CompConstraintContext compConstraint() throws RecognitionException {
		CompConstraintContext _localctx = new CompConstraintContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_compConstraint);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1714);
			componentType();
			setState(1717);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(1715);
				componentID();
				}
				break;
			case OPTIONAL:
				{
				setState(1716);
				multModifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultModifierContext extends ParserRuleContext {
		public TerminalNode OPTIONAL() { return getToken(VtlParser.OPTIONAL, 0); }
		public MultModifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multModifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterMultModifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitMultModifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitMultModifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultModifierContext multModifier() throws RecognitionException {
		MultModifierContext _localctx = new MultModifierContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_multModifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1719);
			match(OPTIONAL);
			setState(1721);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__2 || _la==T__4) {
				{
				setState(1720);
				_la = _input.LA(1);
				if ( !(_la==T__2 || _la==T__4) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RulesetTypeContext extends ParserRuleContext {
		public TerminalNode RULESET() { return getToken(VtlParser.RULESET, 0); }
		public DpRulesetContext dpRuleset() {
			return getRuleContext(DpRulesetContext.class,0);
		}
		public HrRulesetContext hrRuleset() {
			return getRuleContext(HrRulesetContext.class,0);
		}
		public RulesetTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rulesetType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRulesetType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRulesetType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRulesetType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RulesetTypeContext rulesetType() throws RecognitionException {
		RulesetTypeContext _localctx = new RulesetTypeContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_rulesetType);
		try {
			setState(1726);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case RULESET:
				enterOuterAlt(_localctx, 1);
				{
				setState(1723);
				match(RULESET);
				}
				break;
			case DATAPOINT:
			case DATAPOINT_ON_VD:
			case DATAPOINT_ON_VAR:
				enterOuterAlt(_localctx, 2);
				{
				setState(1724);
				dpRuleset();
				}
				break;
			case HIERARCHICAL:
			case HIERARCHICAL_ON_VD:
			case HIERARCHICAL_ON_VAR:
				enterOuterAlt(_localctx, 3);
				{
				setState(1725);
				hrRuleset();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DpRulesetContext extends ParserRuleContext {
		public TerminalNode DATAPOINT() { return getToken(VtlParser.DATAPOINT, 0); }
		public TerminalNode DATAPOINT_ON_VD() { return getToken(VtlParser.DATAPOINT_ON_VD, 0); }
		public ProdValueDomainsContext prodValueDomains() {
			return getRuleContext(ProdValueDomainsContext.class,0);
		}
		public TerminalNode DATAPOINT_ON_VAR() { return getToken(VtlParser.DATAPOINT_ON_VAR, 0); }
		public ProdVariablesContext prodVariables() {
			return getRuleContext(ProdVariablesContext.class,0);
		}
		public DpRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dpRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDpRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDpRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDpRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DpRulesetContext dpRuleset() throws RecognitionException {
		DpRulesetContext _localctx = new DpRulesetContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_dpRuleset);
		try {
			setState(1739);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DATAPOINT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1728);
				match(DATAPOINT);
				}
				break;
			case DATAPOINT_ON_VD:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1729);
				match(DATAPOINT_ON_VD);
				setState(1730);
				match(T__15);
				setState(1731);
				prodValueDomains();
				setState(1732);
				match(T__16);
				}
				}
				break;
			case DATAPOINT_ON_VAR:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1734);
				match(DATAPOINT_ON_VAR);
				setState(1735);
				match(T__15);
				setState(1736);
				prodVariables();
				setState(1737);
				match(T__16);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HrRulesetContext extends ParserRuleContext {
		public TerminalNode HIERARCHICAL() { return getToken(VtlParser.HIERARCHICAL, 0); }
		public TerminalNode HIERARCHICAL_ON_VD() { return getToken(VtlParser.HIERARCHICAL_ON_VD, 0); }
		public TerminalNode IDENTIFIER() { return getToken(VtlParser.IDENTIFIER, 0); }
		public ProdValueDomainsContext prodValueDomains() {
			return getRuleContext(ProdValueDomainsContext.class,0);
		}
		public TerminalNode HIERARCHICAL_ON_VAR() { return getToken(VtlParser.HIERARCHICAL_ON_VAR, 0); }
		public VarIDContext varID() {
			return getRuleContext(VarIDContext.class,0);
		}
		public ProdVariablesContext prodVariables() {
			return getRuleContext(ProdVariablesContext.class,0);
		}
		public HrRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hrRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterHrRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitHrRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitHrRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HrRulesetContext hrRuleset() throws RecognitionException {
		HrRulesetContext _localctx = new HrRulesetContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_hrRuleset);
		int _la;
		try {
			setState(1767);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case HIERARCHICAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(1741);
				match(HIERARCHICAL);
				}
				break;
			case HIERARCHICAL_ON_VD:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1742);
				match(HIERARCHICAL_ON_VD);
				setState(1752);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__15) {
					{
					setState(1743);
					match(T__15);
					setState(1744);
					match(IDENTIFIER);
					setState(1749);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__12) {
						{
						setState(1745);
						match(T__12);
						setState(1746);
						prodValueDomains();
						setState(1747);
						match(T__13);
						}
					}

					setState(1751);
					match(T__16);
					}
				}

				}
				}
				break;
			case HIERARCHICAL_ON_VAR:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1754);
				match(HIERARCHICAL_ON_VAR);
				setState(1765);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__15) {
					{
					setState(1755);
					match(T__15);
					setState(1756);
					varID();
					setState(1761);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__12) {
						{
						setState(1757);
						match(T__12);
						setState(1758);
						prodVariables();
						setState(1759);
						match(T__13);
						}
					}

					setState(1763);
					match(T__16);
					}
				}

				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ProdValueDomainsContext extends ParserRuleContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(VtlParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(VtlParser.IDENTIFIER, i);
		}
		public ProdValueDomainsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prodValueDomains; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterProdValueDomains(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitProdValueDomains(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitProdValueDomains(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ProdValueDomainsContext prodValueDomains() throws RecognitionException {
		ProdValueDomainsContext _localctx = new ProdValueDomainsContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_prodValueDomains);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1769);
			match(IDENTIFIER);
			setState(1774);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(1770);
				match(T__4);
				setState(1771);
				match(IDENTIFIER);
				}
				}
				setState(1776);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ProdVariablesContext extends ParserRuleContext {
		public List<VarIDContext> varID() {
			return getRuleContexts(VarIDContext.class);
		}
		public VarIDContext varID(int i) {
			return getRuleContext(VarIDContext.class,i);
		}
		public ProdVariablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prodVariables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterProdVariables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitProdVariables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitProdVariables(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ProdVariablesContext prodVariables() throws RecognitionException {
		ProdVariablesContext _localctx = new ProdVariablesContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_prodVariables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1777);
			varID();
			setState(1782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(1778);
				match(T__4);
				setState(1779);
				varID();
				}
				}
				setState(1784);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OperatorTypeContext extends ParserRuleContext {
		public List<InputParameterTypeContext> inputParameterType() {
			return getRuleContexts(InputParameterTypeContext.class);
		}
		public InputParameterTypeContext inputParameterType(int i) {
			return getRuleContext(InputParameterTypeContext.class,i);
		}
		public OutputParameterTypeContext outputParameterType() {
			return getRuleContext(OutputParameterTypeContext.class,0);
		}
		public OperatorTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operatorType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterOperatorType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitOperatorType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitOperatorType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OperatorTypeContext operatorType() throws RecognitionException {
		OperatorTypeContext _localctx = new OperatorTypeContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_operatorType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1785);
			inputParameterType();
			setState(1790);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(1786);
				match(T__4);
				setState(1787);
				inputParameterType();
				}
				}
				setState(1792);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1793);
			match(T__17);
			setState(1794);
			outputParameterType();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InputParameterTypeContext extends ParserRuleContext {
		public ScalarTypeContext scalarType() {
			return getRuleContext(ScalarTypeContext.class,0);
		}
		public DatasetTypeContext datasetType() {
			return getRuleContext(DatasetTypeContext.class,0);
		}
		public ComponentTypeContext componentType() {
			return getRuleContext(ComponentTypeContext.class,0);
		}
		public InputParameterTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inputParameterType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterInputParameterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitInputParameterType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitInputParameterType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InputParameterTypeContext inputParameterType() throws RecognitionException {
		InputParameterTypeContext _localctx = new InputParameterTypeContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_inputParameterType);
		try {
			setState(1799);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BOOLEAN:
			case DATE:
			case TIME_PERIOD:
			case NUMBER:
			case STRING:
			case INTEGER:
			case DURATION:
			case SCALAR:
			case IDENTIFIER:
			case TIME:
				enterOuterAlt(_localctx, 1);
				{
				setState(1796);
				scalarType();
				}
				break;
			case DATASET:
				enterOuterAlt(_localctx, 2);
				{
				setState(1797);
				datasetType();
				}
				break;
			case DIMENSION:
			case MEASURE:
			case ATTRIBUTE:
			case VIRAL:
			case COMPONENT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1798);
				componentType();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OutputParameterTypeContext extends ParserRuleContext {
		public ScalarTypeContext scalarType() {
			return getRuleContext(ScalarTypeContext.class,0);
		}
		public DatasetTypeContext datasetType() {
			return getRuleContext(DatasetTypeContext.class,0);
		}
		public ScalarSetTypeContext scalarSetType() {
			return getRuleContext(ScalarSetTypeContext.class,0);
		}
		public RulesetTypeContext rulesetType() {
			return getRuleContext(RulesetTypeContext.class,0);
		}
		public ComponentTypeContext componentType() {
			return getRuleContext(ComponentTypeContext.class,0);
		}
		public OutputParameterTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_outputParameterType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterOutputParameterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitOutputParameterType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitOutputParameterType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OutputParameterTypeContext outputParameterType() throws RecognitionException {
		OutputParameterTypeContext _localctx = new OutputParameterTypeContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_outputParameterType);
		try {
			setState(1806);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BOOLEAN:
			case DATE:
			case TIME_PERIOD:
			case NUMBER:
			case STRING:
			case INTEGER:
			case DURATION:
			case SCALAR:
			case IDENTIFIER:
			case TIME:
				enterOuterAlt(_localctx, 1);
				{
				setState(1801);
				scalarType();
				}
				break;
			case DATASET:
				enterOuterAlt(_localctx, 2);
				{
				setState(1802);
				datasetType();
				}
				break;
			case SET:
				enterOuterAlt(_localctx, 3);
				{
				setState(1803);
				scalarSetType();
				}
				break;
			case DATAPOINT:
			case HIERARCHICAL:
			case RULESET:
			case DATAPOINT_ON_VD:
			case DATAPOINT_ON_VAR:
			case HIERARCHICAL_ON_VD:
			case HIERARCHICAL_ON_VAR:
				enterOuterAlt(_localctx, 4);
				{
				setState(1804);
				rulesetType();
				}
				break;
			case DIMENSION:
			case MEASURE:
			case ATTRIBUTE:
			case VIRAL:
			case COMPONENT:
				enterOuterAlt(_localctx, 5);
				{
				setState(1805);
				componentType();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScalarSetTypeContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(VtlParser.SET, 0); }
		public ScalarTypeContext scalarType() {
			return getRuleContext(ScalarTypeContext.class,0);
		}
		public ScalarSetTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scalarSetType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterScalarSetType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitScalarSetType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitScalarSetType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScalarSetTypeContext scalarSetType() throws RecognitionException {
		ScalarSetTypeContext _localctx = new ScalarSetTypeContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_scalarSetType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1808);
			match(SET);
			setState(1813);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__7) {
				{
				setState(1809);
				match(T__7);
				setState(1810);
				scalarType();
				setState(1811);
				match(T__6);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RetainTypeContext extends ParserRuleContext {
		public TerminalNode BOOLEAN_CONSTANT() { return getToken(VtlParser.BOOLEAN_CONSTANT, 0); }
		public TerminalNode ALL() { return getToken(VtlParser.ALL, 0); }
		public RetainTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_retainType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterRetainType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitRetainType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitRetainType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RetainTypeContext retainType() throws RecognitionException {
		RetainTypeContext _localctx = new RetainTypeContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_retainType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1815);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==BOOLEAN_CONSTANT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefineDatapointRulesetContext extends ParserRuleContext {
		public TerminalNode DEFINE() { return getToken(VtlParser.DEFINE, 0); }
		public TerminalNode DATAPOINT() { return getToken(VtlParser.DATAPOINT, 0); }
		public TerminalNode RULESET() { return getToken(VtlParser.RULESET, 0); }
		public DefineDatapointRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defineDatapointRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefineDatapointRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefineDatapointRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefineDatapointRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefineDatapointRulesetContext defineDatapointRuleset() throws RecognitionException {
		DefineDatapointRulesetContext _localctx = new DefineDatapointRulesetContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_defineDatapointRuleset);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1817);
			match(DEFINE);
			setState(1818);
			match(DATAPOINT);
			setState(1819);
			match(RULESET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefineHierarchicalRulesetContext extends ParserRuleContext {
		public TerminalNode DEFINE() { return getToken(VtlParser.DEFINE, 0); }
		public TerminalNode HIERARCHICAL() { return getToken(VtlParser.HIERARCHICAL, 0); }
		public TerminalNode RULESET() { return getToken(VtlParser.RULESET, 0); }
		public DefineHierarchicalRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defineHierarchicalRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefineHierarchicalRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefineHierarchicalRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefineHierarchicalRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefineHierarchicalRulesetContext defineHierarchicalRuleset() throws RecognitionException {
		DefineHierarchicalRulesetContext _localctx = new DefineHierarchicalRulesetContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_defineHierarchicalRuleset);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1821);
			match(DEFINE);
			setState(1822);
			match(HIERARCHICAL);
			setState(1823);
			match(RULESET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EndDatapointRulesetContext extends ParserRuleContext {
		public TerminalNode END() { return getToken(VtlParser.END, 0); }
		public TerminalNode DATAPOINT() { return getToken(VtlParser.DATAPOINT, 0); }
		public TerminalNode RULESET() { return getToken(VtlParser.RULESET, 0); }
		public EndDatapointRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_endDatapointRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterEndDatapointRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitEndDatapointRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitEndDatapointRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EndDatapointRulesetContext endDatapointRuleset() throws RecognitionException {
		EndDatapointRulesetContext _localctx = new EndDatapointRulesetContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_endDatapointRuleset);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1825);
			match(END);
			setState(1826);
			match(DATAPOINT);
			setState(1827);
			match(RULESET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EndHierarchicalRulesetContext extends ParserRuleContext {
		public TerminalNode END() { return getToken(VtlParser.END, 0); }
		public TerminalNode HIERARCHICAL() { return getToken(VtlParser.HIERARCHICAL, 0); }
		public TerminalNode RULESET() { return getToken(VtlParser.RULESET, 0); }
		public EndHierarchicalRulesetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_endHierarchicalRuleset; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterEndHierarchicalRuleset(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitEndHierarchicalRuleset(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitEndHierarchicalRuleset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EndHierarchicalRulesetContext endHierarchicalRuleset() throws RecognitionException {
		EndHierarchicalRulesetContext _localctx = new EndHierarchicalRulesetContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_endHierarchicalRuleset);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1829);
			match(END);
			setState(1830);
			match(HIERARCHICAL);
			setState(1831);
			match(RULESET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DefineDataStructureContext extends ParserRuleContext {
		public TerminalNode DEFINE() { return getToken(VtlParser.DEFINE, 0); }
		public TerminalNode DATA() { return getToken(VtlParser.DATA, 0); }
		public TerminalNode STRUCTURE() { return getToken(VtlParser.STRUCTURE, 0); }
		public DefineDataStructureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defineDataStructure; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).enterDefineDataStructure(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VtlListener ) ((VtlListener)listener).exitDefineDataStructure(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VtlVisitor ) return ((VtlVisitor<? extends T>)visitor).visitDefineDataStructure(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DefineDataStructureContext defineDataStructure() throws RecognitionException {
		DefineDataStructureContext _localctx = new DefineDataStructureContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_defineDataStructure);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1833);
			match(DEFINE);
			setState(1834);
			match(DATA);
			setState(1835);
			match(STRUCTURE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 4:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 15);
		case 1:
			return precpred(_ctx, 14);
		case 2:
			return precpred(_ctx, 13);
		case 3:
			return precpred(_ctx, 9);
		case 4:
			return precpred(_ctx, 8);
		case 5:
			return precpred(_ctx, 7);
		case 6:
			return precpred(_ctx, 3);
		case 7:
			return precpred(_ctx, 12);
		case 8:
			return precpred(_ctx, 11);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u00fd\u0730\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"+
		"w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"+
		"\4\u0081\t\u0081\3\2\5\2\u0104\n\2\3\2\7\2\u0107\n\2\f\2\16\2\u010a\13"+
		"\2\3\2\7\2\u010d\n\2\f\2\16\2\u0110\13\2\3\2\7\2\u0113\n\2\f\2\16\2\u0116"+
		"\13\2\3\2\5\2\u0119\n\2\3\2\3\2\3\2\5\2\u011e\n\2\3\3\3\3\3\3\5\3\u0123"+
		"\n\3\3\3\3\3\5\3\u0127\n\3\3\4\3\4\3\4\3\4\3\5\3\5\5\5\u012f\n\5\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0139\n\6\3\6\3\6\7\6\u013d\n\6\f\6\16"+
		"\6\u0140\13\6\3\6\3\6\5\6\u0144\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\5\6\u014f\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5"+
		"\6\u015e\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0179\n\6\3\6\3\6\3\6\3"+
		"\6\5\6\u017f\n\6\7\6\u0181\n\6\f\6\16\6\u0184\13\6\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\5\7\u0190\n\7\3\b\3\b\3\b\3\b\5\b\u0196\n\b\3\b\3"+
		"\b\3\b\5\b\u019b\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\7"+
		"\n\u01a9\n\n\f\n\16\n\u01ac\13\n\3\13\3\13\5\13\u01b0\n\13\3\13\3\13\5"+
		"\13\u01b4\n\13\3\13\5\13\u01b7\n\13\3\f\3\f\5\f\u01bb\n\f\3\f\3\f\3\f"+
		"\3\r\3\r\3\r\3\r\5\r\u01c4\n\r\3\r\3\r\3\r\3\r\5\r\u01ca\n\r\7\r\u01cc"+
		"\n\r\f\r\16\r\u01cf\13\r\3\16\3\16\3\16\3\16\5\16\u01d5\n\16\3\16\3\16"+
		"\3\16\7\16\u01da\n\16\f\16\16\16\u01dd\13\16\3\17\5\17\u01e0\n\17\3\17"+
		"\3\17\3\17\3\17\3\17\5\17\u01e7\n\17\3\20\3\20\5\20\u01eb\n\20\3\21\3"+
		"\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\7\22\u01f9\n\22"+
		"\f\22\16\22\u01fc\13\22\3\23\3\23\5\23\u0200\n\23\3\23\3\23\3\23\3\23"+
		"\5\23\u0206\n\23\3\23\3\23\5\23\u020a\n\23\3\23\5\23\u020d\n\23\3\24\3"+
		"\24\3\24\3\24\7\24\u0213\n\24\f\24\16\24\u0216\13\24\3\25\3\25\3\25\5"+
		"\25\u021b\n\25\3\26\3\26\3\26\5\26\u0220\n\26\3\27\3\27\3\27\3\27\3\27"+
		"\3\27\3\27\7\27\u0229\n\27\f\27\16\27\u022c\13\27\5\27\u022e\n\27\3\27"+
		"\3\27\3\27\5\27\u0233\n\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30"+
		"\5\30\u023e\n\30\3\31\3\31\3\31\3\31\5\31\u0244\n\31\3\31\3\31\3\31\5"+
		"\31\u0249\n\31\7\31\u024b\n\31\f\31\16\31\u024e\13\31\5\31\u0250\n\31"+
		"\3\31\3\31\3\32\3\32\3\32\3\32\3\32\5\32\u0259\n\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u0282\n\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u02be"+
		"\n\32\3\32\3\32\5\32\u02c2\n\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\5\32\u02cd\n\32\3\32\3\32\5\32\u02d1\n\32\3\32\3\32\3\32\3\32\3"+
		"\32\3\32\3\32\3\32\3\32\5\32\u02dc\n\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u030b\n\32"+
		"\3\33\3\33\3\33\3\33\3\33\3\33\5\33\u0313\n\33\3\34\3\34\3\34\3\34\7\34"+
		"\u0319\n\34\f\34\16\34\u031c\13\34\3\34\3\34\3\35\3\35\3\35\3\35\7\35"+
		"\u0324\n\35\f\35\16\35\u0327\13\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36"+
		"\3\36\5\36\u0331\n\36\3\36\3\36\3\36\5\36\u0336\n\36\7\36\u0338\n\36\f"+
		"\36\16\36\u033b\13\36\3\36\3\36\3\36\5\36\u0340\n\36\3\36\3\36\5\36\u0344"+
		"\n\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u034e\n\37\3\37\3\37"+
		"\5\37\u0352\n\37\3\37\3\37\3 \3 \3 \5 \u0359\n \3 \3 \3!\3!\3!\3!\3!\3"+
		"!\3!\3\"\3\"\3\"\3\"\3\"\5\"\u0369\n\"\3\"\3\"\3#\3#\3#\3#\3#\5#\u0372"+
		"\n#\3#\3#\3#\5#\u0377\n#\5#\u0379\n#\3#\3#\5#\u037d\n#\3#\3#\3$\3$\3$"+
		"\3$\5$\u0385\n$\3$\5$\u0388\n$\3$\3$\5$\u038c\n$\3$\5$\u038f\n$\3$\3$"+
		"\3%\3%\3%\3%\3%\3%\3%\3%\3%\7%\u039c\n%\f%\16%\u039f\13%\5%\u03a1\n%\3"+
		"%\5%\u03a4\n%\3%\3%\3&\3&\3&\3&\3&\3&\3&\3&\3&\7&\u03b1\n&\f&\16&\u03b4"+
		"\13&\5&\u03b6\n&\3&\3&\5&\u03ba\n&\3&\5&\u03bd\n&\3&\5&\u03c0\n&\3&\5"+
		"&\u03c3\n&\3&\3&\3\'\3\'\3\'\3(\3(\3(\3)\3)\3)\3)\3)\3)\3)\3)\3)\7)\u03d6"+
		"\n)\f)\16)\u03d9\13)\5)\u03db\n)\3)\3)\5)\u03df\n)\3)\3)\5)\u03e3\n)\3"+
		")\3)\5)\u03e7\n)\3)\3)\5)\u03eb\n)\3)\3)\3*\3*\3*\3*\3*\3*\3*\3*\3*\3"+
		"*\5*\u03f9\n*\3+\3+\5+\u03fd\n+\3+\3+\5+\u0401\n+\3+\3+\7+\u0405\n+\f"+
		"+\16+\u0408\13+\3+\3+\3+\5+\u040d\n+\3+\5+\u0410\n+\3+\5+\u0413\n+\3+"+
		"\3+\3+\3,\3,\3,\3,\3,\7,\u041d\n,\f,\16,\u0420\13,\3-\3-\3-\3-\5-\u0426"+
		"\n-\3-\3-\3-\5-\u042b\n-\7-\u042d\n-\f-\16-\u0430\13-\3.\3.\3.\5.\u0435"+
		"\n.\3.\3.\3.\3.\3.\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\5/\u0447\n/\3\60\3"+
		"\60\3\60\3\60\5\60\u044d\n\60\3\60\3\60\3\61\3\61\3\61\5\61\u0454\n\61"+
		"\3\61\3\61\3\61\3\61\5\61\u045a\n\61\7\61\u045c\n\61\f\61\16\61\u045f"+
		"\13\61\3\61\3\61\3\61\3\61\7\61\u0465\n\61\f\61\16\61\u0468\13\61\5\61"+
		"\u046a\n\61\3\62\5\62\u046d\n\62\3\62\3\62\3\62\5\62\u0472\n\62\3\62\3"+
		"\62\5\62\u0476\n\62\3\62\5\62\u0479\n\62\3\63\5\63\u047c\n\63\3\63\3\63"+
		"\3\63\7\63\u0481\n\63\f\63\16\63\u0484\13\63\3\64\3\64\5\64\u0488\n\64"+
		"\3\64\3\64\3\64\5\64\u048d\n\64\3\64\7\64\u0490\n\64\f\64\16\64\u0493"+
		"\13\64\3\65\3\65\3\65\3\65\3\66\5\66\u049a\n\66\3\66\3\66\3\66\3\66\7"+
		"\66\u04a0\n\66\f\66\16\66\u04a3\13\66\3\66\5\66\u04a6\n\66\3\66\5\66\u04a9"+
		"\n\66\3\67\5\67\u04ac\n\67\3\67\3\67\3\67\7\67\u04b1\n\67\f\67\16\67\u04b4"+
		"\13\67\38\38\38\38\39\39\39\39\79\u04be\n9\f9\169\u04c1\139\3:\3:\3:\3"+
		":\7:\u04c7\n:\f:\16:\u04ca\13:\3;\3;\3;\5;\u04cf\n;\3<\3<\3<\3<\5<\u04d5"+
		"\n<\3<\3<\3<\3<\3<\5<\u04dc\n<\3<\3<\3<\3<\3<\5<\u04e3\n<\3<\3<\3<\3<"+
		"\3<\5<\u04ea\n<\3<\3<\7<\u04ee\n<\f<\16<\u04f1\13<\3=\3=\3=\3>\3>\3?\3"+
		"?\3?\7?\u04fb\n?\f?\16?\u04fe\13?\3@\5@\u0501\n@\3@\3@\3@\3@\3A\3A\3A"+
		"\7A\u050a\nA\fA\16A\u050d\13A\3B\5B\u0510\nB\3B\3B\3C\3C\3C\5C\u0517\n"+
		"C\3C\5C\u051a\nC\3D\3D\3D\3E\3E\3E\3E\3E\3E\3E\3E\7E\u0527\nE\fE\16E\u052a"+
		"\13E\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\5F\u0539\nF\3F\3F\3F\3F\3"+
		"F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3"+
		"F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\5F\u0564\nF\3G\3G\3G\3G\7G\u056a"+
		"\nG\fG\16G\u056d\13G\3H\5H\u0570\nH\3H\3H\3H\3H\3I\3I\3I\3I\3I\3I\5I\u057c"+
		"\nI\3J\3J\3J\3J\7J\u0582\nJ\fJ\16J\u0585\13J\3K\3K\3K\3K\3K\5K\u058c\n"+
		"K\3L\3L\3L\3L\7L\u0592\nL\fL\16L\u0595\13L\3M\3M\3M\3M\3M\5M\u059c\nM"+
		"\3N\3N\3N\3N\3N\3O\3O\3O\3O\3O\3P\3P\3P\3P\3P\3P\3P\3P\3P\7P\u05b1\nP"+
		"\fP\16P\u05b4\13P\3Q\3Q\3Q\5Q\u05b9\nQ\3Q\3Q\3Q\5Q\u05be\nQ\5Q\u05c0\n"+
		"Q\3R\3R\3R\3R\3R\7R\u05c7\nR\fR\16R\u05ca\13R\3R\3R\3R\3R\3R\3R\3R\3R"+
		"\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\7R\u05e1\nR\fR\16R\u05e4\13R\3"+
		"R\3R\5R\u05e8\nR\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\5S\u05f4\nS\3S\3S\3T\3"+
		"T\3T\3T\3T\5T\u05fd\nT\3T\3T\3T\3T\5T\u0603\nT\7T\u0605\nT\fT\16T\u0608"+
		"\13T\3T\5T\u060b\nT\3T\5T\u060e\nT\3T\3T\3U\3U\3U\3U\3U\5U\u0617\nU\3"+
		"U\3U\3U\3U\5U\u061d\nU\7U\u061f\nU\fU\16U\u0622\13U\3U\3U\5U\u0626\nU"+
		"\3U\5U\u0629\nU\3V\3V\3W\3W\3W\3W\7W\u0631\nW\fW\16W\u0634\13W\3W\5W\u0637"+
		"\nW\3X\3X\5X\u063b\nX\3X\5X\u063e\nX\3X\3X\5X\u0642\nX\3Y\3Y\3Y\3Y\3Y"+
		"\3Z\3Z\3Z\3Z\3Z\5Z\u064e\nZ\3[\3[\3[\3\\\3\\\3]\3]\3^\3^\3_\3_\3`\3`\3"+
		"a\3a\3b\3b\3c\3c\3d\3d\3e\3e\3f\3f\3f\3f\3f\3f\5f\u066d\nf\3g\3g\3h\3"+
		"h\3i\3i\3i\5i\u0676\ni\3i\5i\u0679\ni\3i\5i\u067c\ni\3i\5i\u067f\ni\3"+
		"j\3j\3k\3k\3l\3l\3m\3m\3m\3m\3m\3m\3m\3m\7m\u068f\nm\fm\16m\u0692\13m"+
		"\3m\3m\5m\u0696\nm\3n\3n\3n\3n\3n\3n\5n\u069e\nn\3o\3o\3o\3o\3o\5o\u06a5"+
		"\no\3p\3p\3p\3p\3p\7p\u06ac\np\fp\16p\u06af\13p\3p\3p\5p\u06b3\np\3q\3"+
		"q\3q\5q\u06b8\nq\3r\3r\5r\u06bc\nr\3s\3s\3s\5s\u06c1\ns\3t\3t\3t\3t\3"+
		"t\3t\3t\3t\3t\3t\3t\5t\u06ce\nt\3u\3u\3u\3u\3u\3u\3u\3u\5u\u06d8\nu\3"+
		"u\5u\u06db\nu\3u\3u\3u\3u\3u\3u\3u\5u\u06e4\nu\3u\3u\5u\u06e8\nu\5u\u06ea"+
		"\nu\3v\3v\3v\7v\u06ef\nv\fv\16v\u06f2\13v\3w\3w\3w\7w\u06f7\nw\fw\16w"+
		"\u06fa\13w\3x\3x\3x\7x\u06ff\nx\fx\16x\u0702\13x\3x\3x\3x\3y\3y\3y\5y"+
		"\u070a\ny\3z\3z\3z\3z\3z\5z\u0711\nz\3{\3{\3{\3{\3{\5{\u0718\n{\3|\3|"+
		"\3}\3}\3}\3}\3~\3~\3~\3~\3\177\3\177\3\177\3\177\3\u0080\3\u0080\3\u0080"+
		"\3\u0080\3\u0081\3\u0081\3\u0081\3\u0081\3\u0081\3\u04ef\3\n\u0082\2\4"+
		"\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNP"+
		"RTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e"+
		"\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6"+
		"\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be"+
		"\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6"+
		"\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee"+
		"\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\2\36\4\2\5\6(("+
		"\3\2\7\b\3\2\5\6\3\2\t\16\3\2\r\16\3\2&\'\3\2*+\3\2jk\3\2\t\r\4\2==\u00c4"+
		"\u00c4\4\2hh\u00ea\u00ea\3\2GH\4\2==ii\5\2==ii\u00db\u00db\3\2\u00d4\u00d9"+
		"\4\2nn\u00cf\u00cf\5\2nnuu\u00ce\u00ce\4\2==\u00d3\u00d3\3\2CD\4\2BB\u0088"+
		"\u008c\5\2EFX[\u0081\u0084\4\2\u00e4\u00e4\u00e7\u00e7\3\2\u00b5\u00b8"+
		"\4\2\u00e4\u00e4\u00e7\u00ea\4\2\u009d\u009e\u00a1\u00a3\6\2\u009d\u00a2"+
		"\u00c5\u00c5\u00dc\u00dc\u00f7\u00f7\4\2\5\5\7\7\4\2==\u00e8\u00e8\2\u07df"+
		"\2\u011d\3\2\2\2\4\u0126\3\2\2\2\6\u0128\3\2\2\2\b\u012e\3\2\2\2\n\u015d"+
		"\3\2\2\2\f\u018f\3\2\2\2\16\u019a\3\2\2\2\20\u019c\3\2\2\2\22\u01a5\3"+
		"\2\2\2\24\u01af\3\2\2\2\26\u01b8\3\2\2\2\30\u01bf\3\2\2\2\32\u01d4\3\2"+
		"\2\2\34\u01df\3\2\2\2\36\u01e8\3\2\2\2 \u01ec\3\2\2\2\"\u01f5\3\2\2\2"+
		"$\u01ff\3\2\2\2&\u020e\3\2\2\2(\u0217\3\2\2\2*\u021f\3\2\2\2,\u0221\3"+
		"\2\2\2.\u0239\3\2\2\2\60\u023f\3\2\2\2\62\u030a\3\2\2\2\64\u0312\3\2\2"+
		"\2\66\u0314\3\2\2\28\u031f\3\2\2\2:\u032a\3\2\2\2<\u0347\3\2\2\2>\u0355"+
		"\3\2\2\2@\u035c\3\2\2\2B\u0363\3\2\2\2D\u036c\3\2\2\2F\u0380\3\2\2\2H"+
		"\u0392\3\2\2\2J\u03a7\3\2\2\2L\u03c6\3\2\2\2N\u03c9\3\2\2\2P\u03cc\3\2"+
		"\2\2R\u03f8\3\2\2\2T\u03fc\3\2\2\2V\u0417\3\2\2\2X\u0421\3\2\2\2Z\u0434"+
		"\3\2\2\2\\\u0446\3\2\2\2^\u0448\3\2\2\2`\u0450\3\2\2\2b\u046c\3\2\2\2"+
		"d\u047b\3\2\2\2f\u0485\3\2\2\2h\u0494\3\2\2\2j\u0499\3\2\2\2l\u04ab\3"+
		"\2\2\2n\u04b5\3\2\2\2p\u04b9\3\2\2\2r\u04c2\3\2\2\2t\u04ce\3\2\2\2v\u04d0"+
		"\3\2\2\2x\u04f2\3\2\2\2z\u04f5\3\2\2\2|\u04f7\3\2\2\2~\u0500\3\2\2\2\u0080"+
		"\u0506\3\2\2\2\u0082\u050f\3\2\2\2\u0084\u0513\3\2\2\2\u0086\u051b\3\2"+
		"\2\2\u0088\u051e\3\2\2\2\u008a\u0563\3\2\2\2\u008c\u0565\3\2\2\2\u008e"+
		"\u056f\3\2\2\2\u0090\u057b\3\2\2\2\u0092\u057d\3\2\2\2\u0094\u058b\3\2"+
		"\2\2\u0096\u058d\3\2\2\2\u0098\u059b\3\2\2\2\u009a\u059d\3\2\2\2\u009c"+
		"\u05a2\3\2\2\2\u009e\u05a7\3\2\2\2\u00a0\u05bf\3\2\2\2\u00a2\u05e7\3\2"+
		"\2\2\u00a4\u05e9\3\2\2\2\u00a6\u05f7\3\2\2\2\u00a8\u0611\3\2\2\2\u00aa"+
		"\u062a\3\2\2\2\u00ac\u062c\3\2\2\2\u00ae\u0638\3\2\2\2\u00b0\u0643\3\2"+
		"\2\2\u00b2\u064d\3\2\2\2\u00b4\u064f\3\2\2\2\u00b6\u0652\3\2\2\2\u00b8"+
		"\u0654\3\2\2\2\u00ba\u0656\3\2\2\2\u00bc\u0658\3\2\2\2\u00be\u065a\3\2"+
		"\2\2\u00c0\u065c\3\2\2\2\u00c2\u065e\3\2\2\2\u00c4\u0660\3\2\2\2\u00c6"+
		"\u0662\3\2\2\2\u00c8\u0664\3\2\2\2\u00ca\u066c\3\2\2\2\u00cc\u066e\3\2"+
		"\2\2\u00ce\u0670\3\2\2\2\u00d0\u0675\3\2\2\2\u00d2\u0680\3\2\2\2\u00d4"+
		"\u0682\3\2\2\2\u00d6\u0684\3\2\2\2\u00d8\u0695\3\2\2\2\u00da\u069d\3\2"+
		"\2\2\u00dc\u069f\3\2\2\2\u00de\u06a6\3\2\2\2\u00e0\u06b4\3\2\2\2\u00e2"+
		"\u06b9\3\2\2\2\u00e4\u06c0\3\2\2\2\u00e6\u06cd\3\2\2\2\u00e8\u06e9\3\2"+
		"\2\2\u00ea\u06eb\3\2\2\2\u00ec\u06f3\3\2\2\2\u00ee\u06fb\3\2\2\2\u00f0"+
		"\u0709\3\2\2\2\u00f2\u0710\3\2\2\2\u00f4\u0712\3\2\2\2\u00f6\u0719\3\2"+
		"\2\2\u00f8\u071b\3\2\2\2\u00fa\u071f\3\2\2\2\u00fc\u0723\3\2\2\2\u00fe"+
		"\u0727\3\2\2\2\u0100\u072b\3\2\2\2\u0102\u0104\5\4\3\2\u0103\u0102\3\2"+
		"\2\2\u0103\u0104\3\2\2\2\u0104\u0108\3\2\2\2\u0105\u0107\7\u00fa\2\2\u0106"+
		"\u0105\3\2\2\2\u0107\u010a\3\2\2\2\u0108\u0106\3\2\2\2\u0108\u0109\3\2"+
		"\2\2\u0109\u010e\3\2\2\2\u010a\u0108\3\2\2\2\u010b\u010d\7\u00fb\2\2\u010c"+
		"\u010b\3\2\2\2\u010d\u0110\3\2\2\2\u010e\u010c\3\2\2\2\u010e\u010f\3\2"+
		"\2\2\u010f\u0111\3\2\2\2\u0110\u010e\3\2\2\2\u0111\u0113\7\u00f9\2\2\u0112"+
		"\u0103\3\2\2\2\u0113\u0116\3\2\2\2\u0114\u0112\3\2\2\2\u0114\u0115\3\2"+
		"\2\2\u0115\u0118\3\2\2\2\u0116\u0114\3\2\2\2\u0117\u0119\5\4\3\2\u0118"+
		"\u0117\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011a\3\2\2\2\u011a\u011e\7\2"+
		"\2\3\u011b\u011e\7\u00fa\2\2\u011c\u011e\7\u00fb\2\2\u011d\u0114\3\2\2"+
		"\2\u011d\u011b\3\2\2\2\u011d\u011c\3\2\2\2\u011e\3\3\2\2\2\u011f\u0120"+
		"\5\u00c0a\2\u0120\u0121\7\25\2\2\u0121\u0123\3\2\2\2\u0122\u011f\3\2\2"+
		"\2\u0122\u0123\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0127\5\n\6\2\u0125\u0127"+
		"\5\6\4\2\u0126\u0122\3\2\2\2\u0126\u0125\3\2\2\2\u0127\5\3\2\2\2\u0128"+
		"\u0129\5\u00c0a\2\u0129\u012a\7q\2\2\u012a\u012b\5\n\6\2\u012b\7\3\2\2"+
		"\2\u012c\u012f\5\n\6\2\u012d\u012f\7h\2\2\u012e\u012c\3\2\2\2\u012e\u012d"+
		"\3\2\2\2\u012f\t\3\2\2\2\u0130\u0131\b\6\1\2\u0131\u013e\5\62\32\2\u0132"+
		"\u0138\7\3\2\2\u0133\u0139\5R*\2\u0134\u0135\5\n\6\2\u0135\u0136\7\25"+
		"\2\2\u0136\u0137\5\n\6\2\u0137\u0139\3\2\2\2\u0138\u0133\3\2\2\2\u0138"+
		"\u0134\3\2\2\2\u0139\u013a\3\2\2\2\u013a\u013b\7\4\2\2\u013b\u013d\3\2"+
		"\2\2\u013c\u0132\3\2\2\2\u013d\u0140\3\2\2\2\u013e\u013c\3\2\2\2\u013e"+
		"\u013f\3\2\2\2\u013f\u0143\3\2\2\2\u0140\u013e\3\2\2\2\u0141\u0142\7\26"+
		"\2\2\u0142\u0144\5\u00c2b\2\u0143\u0141\3\2\2\2\u0143\u0144\3\2\2\2\u0144"+
		"\u015e\3\2\2\2\u0145\u0146\t\2\2\2\u0146\u015e\5\n\6\22\u0147\u0148\7"+
		"8\2\2\u0148\u0149\7\17\2\2\u0149\u014a\5\n\6\2\u014a\u014b\7\63\2\2\u014b"+
		"\u014e\5\n\6\2\u014c\u014d\7\63\2\2\u014d\u014f\5\u00f6|\2\u014e\u014c"+
		"\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u0151\7\20\2\2"+
		"\u0151\u015e\3\2\2\2\u0152\u0153\7\30\2\2\u0153\u0154\5\n\6\2\u0154\u0155"+
		"\7\31\2\2\u0155\u0156\5\n\6\2\u0156\u0157\7\32\2\2\u0157\u0158\5\n\6\b"+
		"\u0158\u015e\3\2\2\2\u0159\u015e\5\f\7\2\u015a\u015e\5\62\32\2\u015b\u015e"+
		"\5\u00ccg\2\u015c\u015e\7\u00eb\2\2\u015d\u0130\3\2\2\2\u015d\u0145\3"+
		"\2\2\2\u015d\u0147\3\2\2\2\u015d\u0152\3\2\2\2\u015d\u0159\3\2\2\2\u015d"+
		"\u015a\3\2\2\2\u015d\u015b\3\2\2\2\u015d\u015c\3\2\2\2\u015e\u0182\3\2"+
		"\2\2\u015f\u0160\f\21\2\2\u0160\u0161\t\3\2\2\u0161\u0181\5\n\6\22\u0162"+
		"\u0163\f\20\2\2\u0163\u0164\t\4\2\2\u0164\u0181\5\n\6\21\u0165\u0166\f"+
		"\17\2\2\u0166\u0167\t\5\2\2\u0167\u0181\5\n\6\20\u0168\u0169\f\13\2\2"+
		"\u0169\u016a\t\6\2\2\u016a\u0181\5\n\6\f\u016b\u016c\f\n\2\2\u016c\u016d"+
		"\7%\2\2\u016d\u0181\5\n\6\13\u016e\u016f\f\t\2\2\u016f\u0170\t\7\2\2\u0170"+
		"\u0181\5\n\6\n\u0171\u0172\f\5\2\2\u0172\u0173\7S\2\2\u0173\u0181\5\n"+
		"\6\6\u0174\u0175\f\16\2\2\u0175\u0178\t\b\2\2\u0176\u0179\58\35\2\u0177"+
		"\u0179\7\u00eb\2\2\u0178\u0176\3\2\2\2\u0178\u0177\3\2\2\2\u0179\u0181"+
		"\3\2\2\2\u017a\u017b\f\r\2\2\u017b\u017c\78\2\2\u017c\u017e\5\n\6\2\u017d"+
		"\u017f\7=\2\2\u017e\u017d\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u0181\3\2"+
		"\2\2\u0180\u015f\3\2\2\2\u0180\u0162\3\2\2\2\u0180\u0165\3\2\2\2\u0180"+
		"\u0168\3\2\2\2\u0180\u016b\3\2\2\2\u0180\u016e\3\2\2\2\u0180\u0171\3\2"+
		"\2\2\u0180\u0174\3\2\2\2\u0180\u017a\3\2\2\2\u0181\u0184\3\2\2\2\u0182"+
		"\u0180\3\2\2\2\u0182\u0183\3\2\2\2\u0183\13\3\2\2\2\u0184\u0182\3\2\2"+
		"\2\u0185\u0190\5F$\2\u0186\u0190\5*\26\2\u0187\u0190\5\u00a6T\2\u0188"+
		"\u0190\5\u00a8U\2\u0189\u0190\5T+\2\u018a\u0190\5\u008aF\2\u018b\u0190"+
		"\5\16\b\2\u018c\u0190\5\u00a2R\2\u018d\u0190\5\60\31\2\u018e\u0190\5^"+
		"\60\2\u018f\u0185\3\2\2\2\u018f\u0186\3\2\2\2\u018f\u0187\3\2\2\2\u018f"+
		"\u0188\3\2\2\2\u018f\u0189\3\2\2\2\u018f\u018a\3\2\2\2\u018f\u018b\3\2"+
		"\2\2\u018f\u018c\3\2\2\2\u018f\u018d\3\2\2\2\u018f\u018e\3\2\2\2\u0190"+
		"\r\3\2\2\2\u0191\u019b\5B\"\2\u0192\u0195\5> \2\u0193\u0194\t\5\2\2\u0194"+
		"\u0196\5\n\6\2\u0195\u0193\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u019b\3\2"+
		"\2\2\u0197\u019b\5@!\2\u0198\u019b\5D#\2\u0199\u019b\7\35\2\2\u019a\u0191"+
		"\3\2\2\2\u019a\u0192\3\2\2\2\u019a\u0197\3\2\2\2\u019a\u0198\3\2\2\2\u019a"+
		"\u0199\3\2\2\2\u019b\17\3\2\2\2\u019c\u019d\5\u00fa~\2\u019d\u019e\5\u00be"+
		"`\2\u019e\u019f\7\17\2\2\u019f\u01a0\5\26\f\2\u01a0\u01a1\7\20\2\2\u01a1"+
		"\u01a2\7\u00ac\2\2\u01a2\u01a3\5\22\n\2\u01a3\u01a4\5\u00fe\u0080\2\u01a4"+
		"\21\3\2\2\2\u01a5\u01aa\5\24\13\2\u01a6\u01a7\7\u00f9\2\2\u01a7\u01a9"+
		"\5\24\13\2\u01a8\u01a6\3\2\2\2\u01a9\u01ac\3\2\2\2\u01aa\u01a8\3\2\2\2"+
		"\u01aa\u01ab\3\2\2\2\u01ab\23\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ad\u01ae"+
		"\7\u00eb\2\2\u01ae\u01b0\7\21\2\2\u01af\u01ad\3\2\2\2\u01af\u01b0\3\2"+
		"\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b3\5\32\16\2\u01b2\u01b4\5L\'\2\u01b3"+
		"\u01b2\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4\u01b6\3\2\2\2\u01b5\u01b7\5N"+
		"(\2\u01b6\u01b5\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\25\3\2\2\2\u01b8\u01ba"+
		"\t\t\2\2\u01b9\u01bb\5\30\r\2\u01ba\u01b9\3\2\2\2\u01ba\u01bb\3\2\2\2"+
		"\u01bb\u01bc\3\2\2\2\u01bc\u01bd\7u\2\2\u01bd\u01be\7\u00eb\2\2\u01be"+
		"\27\3\2\2\2\u01bf\u01c0\7\u009c\2\2\u01c0\u01c3\7\u00eb\2\2\u01c1\u01c2"+
		"\7$\2\2\u01c2\u01c4\7\u00eb\2\2\u01c3\u01c1\3\2\2\2\u01c3\u01c4\3\2\2"+
		"\2\u01c4\u01cd\3\2\2\2\u01c5\u01c6\7\63\2\2\u01c6\u01c9\7\u00eb\2\2\u01c7"+
		"\u01c8\7$\2\2\u01c8\u01ca\7\u00eb\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca"+
		"\3\2\2\2\u01ca\u01cc\3\2\2\2\u01cb\u01c5\3\2\2\2\u01cc\u01cf\3\2\2\2\u01cd"+
		"\u01cb\3\2\2\2\u01cd\u01ce\3\2\2\2\u01ce\31\3\2\2\2\u01cf\u01cd\3\2\2"+
		"\2\u01d0\u01d1\7\u00ad\2\2\u01d1\u01d2\5\n\6\2\u01d2\u01d3\7\31\2\2\u01d3"+
		"\u01d5\3\2\2\2\u01d4\u01d0\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5\u01d6\3\2"+
		"\2\2\u01d6\u01d7\5\36\20\2\u01d7\u01db\5\34\17\2\u01d8\u01da\5\34\17\2"+
		"\u01d9\u01d8\3\2\2\2\u01da\u01dd\3\2\2\2\u01db\u01d9\3\2\2\2\u01db\u01dc"+
		"\3\2\2\2\u01dc\33\3\2\2\2\u01dd\u01db\3\2\2\2\u01de\u01e0\t\4\2\2\u01df"+
		"\u01de\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e6\7\u00eb"+
		"\2\2\u01e2\u01e3\7\3\2\2\u01e3\u01e4\5\n\6\2\u01e4\u01e5\7\4\2\2\u01e5"+
		"\u01e7\3\2\2\2\u01e6\u01e2\3\2\2\2\u01e6\u01e7\3\2\2\2\u01e7\35\3\2\2"+
		"\2\u01e8\u01ea\7\u00eb\2\2\u01e9\u01eb\t\n\2\2\u01ea\u01e9\3\2\2\2\u01ea"+
		"\u01eb\3\2\2\2\u01eb\37\3\2\2\2\u01ec\u01ed\5\u00f8}\2\u01ed\u01ee\5\u00be"+
		"`\2\u01ee\u01ef\7\17\2\2\u01ef\u01f0\5&\24\2\u01f0\u01f1\7\20\2\2\u01f1"+
		"\u01f2\7\u00ac\2\2\u01f2\u01f3\5\"\22\2\u01f3\u01f4\5\u00fc\177\2\u01f4"+
		"!\3\2\2\2\u01f5\u01fa\5$\23\2\u01f6\u01f7\7\u00f9\2\2\u01f7\u01f9\5$\23"+
		"\2\u01f8\u01f6\3\2\2\2\u01f9\u01fc\3\2\2\2\u01fa\u01f8\3\2\2\2\u01fa\u01fb"+
		"\3\2\2\2\u01fb#\3\2\2\2\u01fc\u01fa\3\2\2\2\u01fd\u01fe\7\u00eb\2\2\u01fe"+
		"\u0200\7\21\2\2\u01ff\u01fd\3\2\2\2\u01ff\u0200\3\2\2\2\u0200\u0205\3"+
		"\2\2\2\u0201\u0202\7\u00ad\2\2\u0202\u0203\5\n\6\2\u0203\u0204\7\31\2"+
		"\2\u0204\u0206\3\2\2\2\u0205\u0201\3\2\2\2\u0205\u0206\3\2\2\2\u0206\u0207"+
		"\3\2\2\2\u0207\u0209\5\n\6\2\u0208\u020a\5L\'\2\u0209\u0208\3\2\2\2\u0209"+
		"\u020a\3\2\2\2\u020a\u020c\3\2\2\2\u020b\u020d\5N(\2\u020c\u020b\3\2\2"+
		"\2\u020c\u020d\3\2\2\2\u020d%\3\2\2\2\u020e\u020f\t\t\2\2\u020f\u0214"+
		"\5(\25\2\u0210\u0211\7\63\2\2\u0211\u0213\5(\25\2\u0212\u0210\3\2\2\2"+
		"\u0213\u0216\3\2\2\2\u0214\u0212\3\2\2\2\u0214\u0215\3\2\2\2\u0215\'\3"+
		"\2\2\2\u0216\u0214\3\2\2\2\u0217\u021a\5\u00c0a\2\u0218\u0219\7$\2\2\u0219"+
		"\u021b\7\u00eb\2\2\u021a\u0218\3\2\2\2\u021a\u021b\3\2\2\2\u021b)\3\2"+
		"\2\2\u021c\u0220\5,\27\2\u021d\u0220\5 \21\2\u021e\u0220\5\20\t\2\u021f"+
		"\u021c\3\2\2\2\u021f\u021d\3\2\2\2\u021f\u021e\3\2\2\2\u0220+\3\2\2\2"+
		"\u0221\u0222\7p\2\2\u0222\u0223\7o\2\2\u0223\u0224\5\u00c4c\2\u0224\u022d"+
		"\7\17\2\2\u0225\u022a\5.\30\2\u0226\u0227\7\63\2\2\u0227\u0229\5.\30\2"+
		"\u0228\u0226\3\2\2\2\u0229\u022c\3\2\2\2\u022a\u0228\3\2\2\2\u022a\u022b"+
		"\3\2\2\2\u022b\u022e\3\2\2\2\u022c\u022a\3\2\2\2\u022d\u0225\3\2\2\2\u022d"+
		"\u022e\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0232\7\20\2\2\u0230\u0231\7"+
		"\u00bd\2\2\u0231\u0233\5\u00dan\2\u0232\u0230\3\2\2\2\u0232\u0233\3\2"+
		"\2\2\u0233\u0234\3\2\2\2\u0234\u0235\7\u00ac\2\2\u0235\u0236\5\n\6\2\u0236"+
		"\u0237\7v\2\2\u0237\u0238\7o\2\2\u0238-\3\2\2\2\u0239\u023a\5\u00c0a\2"+
		"\u023a\u023d\5\u00dan\2\u023b\u023c\7\u00d0\2\2\u023c\u023e\5\u00ccg\2"+
		"\u023d\u023b\3\2\2\2\u023d\u023e\3\2\2\2\u023e/\3\2\2\2\u023f\u0240\5"+
		"\u00c4c\2\u0240\u024f\7\17\2\2\u0241\u0244\5\u00ccg\2\u0242\u0244\7h\2"+
		"\2\u0243\u0241\3\2\2\2\u0243\u0242\3\2\2\2\u0244\u024c\3\2\2\2\u0245\u0248"+
		"\7\63\2\2\u0246\u0249\5\u00ccg\2\u0247\u0249\7h\2\2\u0248\u0246\3\2\2"+
		"\2\u0248\u0247\3\2\2\2\u0249\u024b\3\2\2\2\u024a\u0245\3\2\2\2\u024b\u024e"+
		"\3\2\2\2\u024c\u024a\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u0250\3\2\2\2\u024e"+
		"\u024c\3\2\2\2\u024f\u0243\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0251\3\2"+
		"\2\2\u0251\u0252\7\20\2\2\u0252\61\3\2\2\2\u0253\u0254\7O\2\2\u0254\u0255"+
		"\7\17\2\2\u0255\u0258\5\n\6\2\u0256\u0257\7\63\2\2\u0257\u0259\5\b\5\2"+
		"\u0258\u0256\3\2\2\2\u0258\u0259\3\2\2\2\u0259\u025a\3\2\2\2\u025a\u025b"+
		"\7\20\2\2\u025b\u030b\3\2\2\2\u025c\u025d\7|\2\2\u025d\u025e\7\17\2\2"+
		"\u025e\u025f\5\n\6\2\u025f\u0260\7\20\2\2\u0260\u030b\3\2\2\2\u0261\u0262"+
		"\7}\2\2\u0262\u0263\7\17\2\2\u0263\u0264\5\n\6\2\u0264\u0265\7\20\2\2"+
		"\u0265\u030b\3\2\2\2\u0266\u0267\7J\2\2\u0267\u0268\7\17\2\2\u0268\u0269"+
		"\5\n\6\2\u0269\u026a\7\20\2\2\u026a\u030b\3\2\2\2\u026b\u026c\7a\2\2\u026c"+
		"\u026d\7\17\2\2\u026d\u026e\5\n\6\2\u026e\u026f\7\20\2\2\u026f\u030b\3"+
		"\2\2\2\u0270\u0271\7L\2\2\u0271\u0272\7\17\2\2\u0272\u0273\5\n\6\2\u0273"+
		"\u0274\7\20\2\2\u0274\u030b\3\2\2\2\u0275\u0276\7M\2\2\u0276\u0277\7\17"+
		"\2\2\u0277\u0278\5\n\6\2\u0278\u0279\7\63\2\2\u0279\u027a\5\n\6\2\u027a"+
		"\u027b\7\20\2\2\u027b\u030b\3\2\2\2\u027c\u027d\7N\2\2\u027d\u027e\7\17"+
		"\2\2\u027e\u0281\5\n\6\2\u027f\u0280\7\63\2\2\u0280\u0282\5\b\5\2\u0281"+
		"\u027f\3\2\2\2\u0281\u0282\3\2\2\2\u0282\u0283\3\2\2\2\u0283\u0284\7\20"+
		"\2\2\u0284\u030b\3\2\2\2\u0285\u0286\7P\2\2\u0286\u0287\7\17\2\2\u0287"+
		"\u0288\5\n\6\2\u0288\u0289\7\63\2\2\u0289\u028a\5\n\6\2\u028a\u028b\7"+
		"\20\2\2\u028b\u030b\3\2\2\2\u028c\u028d\7~\2\2\u028d\u028e\7\17\2\2\u028e"+
		"\u028f\5\n\6\2\u028f\u0290\7\20\2\2\u0290\u030b\3\2\2\2\u0291\u0292\7"+
		"R\2\2\u0292\u0293\7\17\2\2\u0293\u0294\5\n\6\2\u0294\u0295\7\20\2\2\u0295"+
		"\u030b\3\2\2\2\u0296\u0297\7)\2\2\u0297\u0298\7\17\2\2\u0298\u0299\5\n"+
		"\6\2\u0299\u029a\7\63\2\2\u029a\u029b\5\n\6\2\u029b\u029c\7\63\2\2\u029c"+
		"\u029d\5\n\6\2\u029d\u029e\7\20\2\2\u029e\u030b\3\2\2\2\u029f\u02a0\7"+
		"T\2\2\u02a0\u02a1\7\17\2\2\u02a1\u02a2\5\n\6\2\u02a2\u02a3\7\20\2\2\u02a3"+
		"\u030b\3\2\2\2\u02a4\u02a5\7x\2\2\u02a5\u02a6\7\17\2\2\u02a6\u02a7\5\n"+
		"\6\2\u02a7\u02a8\7\20\2\2\u02a8\u030b\3\2\2\2\u02a9\u02aa\7y\2\2\u02aa"+
		"\u02ab\7\17\2\2\u02ab\u02ac\5\n\6\2\u02ac\u02ad\7\20\2\2\u02ad\u030b\3"+
		"\2\2\2\u02ae\u02af\7U\2\2\u02af\u02b0\7\17\2\2\u02b0\u02b1\5\n\6\2\u02b1"+
		"\u02b2\7\20\2\2\u02b2\u030b\3\2\2\2\u02b3\u02b4\7V\2\2\u02b4\u02b5\7\17"+
		"\2\2\u02b5\u02b6\5\n\6\2\u02b6\u02b7\7\20\2\2\u02b7\u030b\3\2\2\2\u02b8"+
		"\u02b9\7W\2\2\u02b9\u02ba\7\17\2\2\u02ba\u02bd\5\n\6\2\u02bb\u02bc\7\63"+
		"\2\2\u02bc\u02be\5\b\5\2\u02bd\u02bb\3\2\2\2\u02bd\u02be\3\2\2\2\u02be"+
		"\u02c1\3\2\2\2\u02bf\u02c0\7\63\2\2\u02c0\u02c2\5\b\5\2\u02c1\u02bf\3"+
		"\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u02c3\3\2\2\2\u02c3\u02c4\7\20\2\2\u02c4"+
		"\u030b\3\2\2\2\u02c5\u02c6\7z\2\2\u02c6\u02c7\7\17\2\2\u02c7\u02c8\5\n"+
		"\6\2\u02c8\u02c9\7\63\2\2\u02c9\u02cc\5\n\6\2\u02ca\u02cb\7\63\2\2\u02cb"+
		"\u02cd\5\b\5\2\u02cc\u02ca\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cd\u02d0\3\2"+
		"\2\2\u02ce\u02cf\7\63\2\2\u02cf\u02d1\5\b\5\2\u02d0\u02ce\3\2\2\2\u02d0"+
		"\u02d1\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2\u02d3\7\20\2\2\u02d3\u030b\3"+
		"\2\2\2\u02d4\u02d5\7{\2\2\u02d5\u02d6\7\17\2\2\u02d6\u02d7\5\n\6\2\u02d7"+
		"\u02d8\7\63\2\2\u02d8\u02db\5\n\6\2\u02d9\u02da\7\63\2\2\u02da\u02dc\5"+
		"\b\5\2\u02db\u02d9\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc\u02dd\3\2\2\2\u02dd"+
		"\u02de\7\20\2\2\u02de\u030b\3\2\2\2\u02df\u02e0\7d\2\2\u02e0\u02e1\7\17"+
		"\2\2\u02e1\u02e2\5\n\6\2\u02e2\u02e3\7\63\2\2\u02e3\u02e4\5\n\6\2\u02e4"+
		"\u02e5\7\20\2\2\u02e5\u030b\3\2\2\2\u02e6\u02e7\7,\2\2\u02e7\u02e8\7\17"+
		"\2\2\u02e8\u02e9\5\n\6\2\u02e9\u02ea\7\20\2\2\u02ea\u030b\3\2\2\2\u02eb"+
		"\u02ec\7f\2\2\u02ec\u02ed\7\17\2\2\u02ed\u02ee\5\n\6\2\u02ee\u02ef\7\63"+
		"\2\2\u02ef\u02f0\5\n\6\2\u02f0\u02f1\7\20\2\2\u02f1\u030b\3\2\2\2\u02f2"+
		"\u02f3\7Q\2\2\u02f3\u02f4\7\17\2\2\u02f4\u02f5\5\n\6\2\u02f5\u02f6\7\63"+
		"\2\2\u02f6\u02f7\5\n\6\2\u02f7\u02f8\7\20\2\2\u02f8\u030b\3\2\2\2\u02f9"+
		"\u030b\5\64\33\2\u02fa\u030b\5:\36\2\u02fb\u030b\5<\37\2\u02fc\u030b\5"+
		"P)\2\u02fd\u02fe\7\u0097\2\2\u02fe\u02ff\7\17\2\2\u02ff\u0300\5\n\6\2"+
		"\u0300\u0301\7\20\2\2\u0301\u030b\3\2\2\2\u0302\u0303\7\u0098\2\2\u0303"+
		"\u0304\7\17\2\2\u0304\u0305\5\n\6\2\u0305\u0306\7\20\2\2\u0306\u030b\3"+
		"\2\2\2\u0307\u030b\5H%\2\u0308\u030b\5J&\2\u0309\u030b\5F$\2\u030a\u0253"+
		"\3\2\2\2\u030a\u025c\3\2\2\2\u030a\u0261\3\2\2\2\u030a\u0266\3\2\2\2\u030a"+
		"\u026b\3\2\2\2\u030a\u0270\3\2\2\2\u030a\u0275\3\2\2\2\u030a\u027c\3\2"+
		"\2\2\u030a\u0285\3\2\2\2\u030a\u028c\3\2\2\2\u030a\u0291\3\2\2\2\u030a"+
		"\u0296\3\2\2\2\u030a\u029f\3\2\2\2\u030a\u02a4\3\2\2\2\u030a\u02a9\3\2"+
		"\2\2\u030a\u02ae\3\2\2\2\u030a\u02b3\3\2\2\2\u030a\u02b8\3\2\2\2\u030a"+
		"\u02c5\3\2\2\2\u030a\u02d4\3\2\2\2\u030a\u02df\3\2\2\2\u030a\u02e6\3\2"+
		"\2\2\u030a\u02eb\3\2\2\2\u030a\u02f2\3\2\2\2\u030a\u02f9\3\2\2\2\u030a"+
		"\u02fa\3\2\2\2\u030a\u02fb\3\2\2\2\u030a\u02fc\3\2\2\2\u030a\u02fd\3\2"+
		"\2\2\u030a\u0302\3\2\2\2\u030a\u0307\3\2\2\2\u030a\u0308\3\2\2\2\u030a"+
		"\u0309\3\2\2\2\u030b\63\3\2\2\2\u030c\u030d\7\17\2\2\u030d\u030e\5\n\6"+
		"\2\u030e\u030f\7\20\2\2\u030f\u0313\3\2\2\2\u0310\u0313\5\u00c0a\2\u0311"+
		"\u0313\5\u00ccg\2\u0312\u030c\3\2\2\2\u0312\u0310\3\2\2\2\u0312\u0311"+
		"\3\2\2\2\u0313\65\3\2\2\2\u0314\u0315\7\3\2\2\u0315\u031a\7\u00eb\2\2"+
		"\u0316\u0317\7\63\2\2\u0317\u0319\7\u00eb\2\2\u0318\u0316\3\2\2\2\u0319"+
		"\u031c\3\2\2\2\u031a\u0318\3\2\2\2\u031a\u031b\3\2\2\2\u031b\u031d\3\2"+
		"\2\2\u031c\u031a\3\2\2\2\u031d\u031e\7\4\2\2\u031e\67\3\2\2\2\u031f\u0320"+
		"\7\22\2\2\u0320\u0325\5\u00ccg\2\u0321\u0322\7\63\2\2\u0322\u0324\5\u00cc"+
		"g\2\u0323\u0321\3\2\2\2\u0324\u0327\3\2\2\2\u0325\u0323\3\2\2\2\u0325"+
		"\u0326\3\2\2\2\u0326\u0328\3\2\2\2\u0327\u0325\3\2\2\2\u0328\u0329\7\23"+
		"\2\2\u03299\3\2\2\2\u032a\u032b\7\27\2\2\u032b\u032c\7\17\2\2\u032c\u032d"+
		"\5\u00c6d\2\u032d\u0330\7\17\2\2\u032e\u0331\5\u00c2b\2\u032f\u0331\5"+
		"\u00ccg\2\u0330\u032e\3\2\2\2\u0330\u032f\3\2\2\2\u0330\u0331\3\2\2\2"+
		"\u0331\u0339\3\2\2\2\u0332\u0335\7\63\2\2\u0333\u0336\5\u00c2b\2\u0334"+
		"\u0336\5\u00ccg\2\u0335\u0333\3\2\2\2\u0335\u0334\3\2\2\2\u0336\u0338"+
		"\3\2\2\2\u0337\u0332\3\2\2\2\u0338\u033b\3\2\2\2\u0339\u0337\3\2\2\2\u0339"+
		"\u033a\3\2\2\2\u033a\u033c\3\2\2\2\u033b\u0339\3\2\2\2\u033c\u033f\7\20"+
		"\2\2\u033d\u033e\7\u00e3\2\2\u033e\u0340\7\u00ea\2\2\u033f\u033d\3\2\2"+
		"\2\u033f\u0340\3\2\2\2\u0340\u0343\3\2\2\2\u0341\u0342\7\u00bd\2\2\u0342"+
		"\u0344\5\u00f2z\2\u0343\u0341\3\2\2\2\u0343\u0344\3\2\2\2\u0344\u0345"+
		"\3\2\2\2\u0345\u0346\7\20\2\2\u0346;\3\2\2\2\u0347\u0348\7\u00cd\2\2\u0348"+
		"\u0349\7\17\2\2\u0349\u034a\5\n\6\2\u034a\u034d\7\63\2\2\u034b\u034e\5"+
		"\u00d2j\2\u034c\u034e\5\u00d4k\2\u034d\u034b\3\2\2\2\u034d\u034c\3\2\2"+
		"\2\u034e\u0351\3\2\2\2\u034f\u0350\7\63\2\2\u0350\u0352\7\u00ea\2\2\u0351"+
		"\u034f\3\2\2\2\u0351\u0352\3\2\2\2\u0352\u0353\3\2\2\2\u0353\u0354\7\20"+
		"\2\2\u0354=\3\2\2\2\u0355\u0356\7\u00c3\2\2\u0356\u0358\7\17\2\2\u0357"+
		"\u0359\5\n\6\2\u0358\u0357\3\2\2\2\u0358\u0359\3\2\2\2\u0359\u035a\3\2"+
		"\2\2\u035a\u035b\7\20\2\2\u035b?\3\2\2\2\u035c\u035d\7\u0099\2\2\u035d"+
		"\u035e\7\17\2\2\u035e\u035f\5\n\6\2\u035f\u0360\7\63\2\2\u0360\u0361\7"+
		"\u00e4\2\2\u0361\u0362\7\20\2\2\u0362A\3\2\2\2\u0363\u0364\7\u0096\2\2"+
		"\u0364\u0365\7\17\2\2\u0365\u0368\5\n\6\2\u0366\u0367\7\63\2\2\u0367\u0369"+
		"\t\13\2\2\u0368\u0366\3\2\2\2\u0368\u0369\3\2\2\2\u0369\u036a\3\2\2\2"+
		"\u036a\u036b\7\20\2\2\u036bC\3\2\2\2\u036c\u036d\7\u00c6\2\2\u036d\u036e"+
		"\7\17\2\2\u036e\u0371\7\u00ea\2\2\u036f\u0370\7\63\2\2\u0370\u0372\t\f"+
		"\2\2\u0371\u036f\3\2\2\2\u0371\u0372\3\2\2\2\u0372\u0378\3\2\2\2\u0373"+
		"\u0376\7\63\2\2\u0374\u0377\5\n\6\2\u0375\u0377\7h\2\2\u0376\u0374\3\2"+
		"\2\2\u0376\u0375\3\2\2\2\u0377\u0379\3\2\2\2\u0378\u0373\3\2\2\2\u0378"+
		"\u0379\3\2\2\2\u0379\u037c\3\2\2\2\u037a\u037b\7\63\2\2\u037b\u037d\t"+
		"\r\2\2\u037c\u037a\3\2\2\2\u037c\u037d\3\2\2\2\u037d\u037e\3\2\2\2\u037e"+
		"\u037f\7\20\2\2\u037fE\3\2\2\2\u0380\u0381\7\67\2\2\u0381\u0382\7\17\2"+
		"\2\u0382\u0384\5\n\6\2\u0383\u0385\5L\'\2\u0384\u0383\3\2\2\2\u0384\u0385"+
		"\3\2\2\2\u0385\u0387\3\2\2\2\u0386\u0388\5N(\2\u0387\u0386\3\2\2\2\u0387"+
		"\u0388\3\2\2\2\u0388\u038b\3\2\2\2\u0389\u038a\7;\2\2\u038a\u038c\5\n"+
		"\6\2\u038b\u0389\3\2\2\2\u038b\u038c\3\2\2\2\u038c\u038e\3\2\2\2\u038d"+
		"\u038f\t\16\2\2\u038e\u038d\3\2\2\2\u038e\u038f\3\2\2\2\u038f\u0390\3"+
		"\2\2\2\u0390\u0391\7\20\2\2\u0391G\3\2\2\2\u0392\u0393\7\u00d1\2\2\u0393"+
		"\u0394\7\17\2\2\u0394\u0395\5\n\6\2\u0395\u0396\7\63\2\2\u0396\u03a0\7"+
		"\u00eb\2\2\u0397\u0398\7\u00da\2\2\u0398\u039d\5\u00c2b\2\u0399\u039a"+
		"\7\63\2\2\u039a\u039c\5\u00c2b\2\u039b\u0399\3\2\2\2\u039c\u039f\3\2\2"+
		"\2\u039d\u039b\3\2\2\2\u039d\u039e\3\2\2\2\u039e\u03a1\3\2\2\2\u039f\u039d"+
		"\3\2\2\2\u03a0\u0397\3\2\2\2\u03a0\u03a1\3\2\2\2\u03a1\u03a3\3\2\2\2\u03a2"+
		"\u03a4\t\17\2\2\u03a3\u03a2\3\2\2\2\u03a3\u03a4\3\2\2\2\u03a4\u03a5\3"+
		"\2\2\2\u03a5\u03a6\7\20\2\2\u03a6I\3\2\2\2\u03a7\u03a8\7\u00d2\2\2\u03a8"+
		"\u03a9\7\17\2\2\u03a9\u03aa\5\n\6\2\u03aa\u03ab\7\63\2\2\u03ab\u03b5\7"+
		"\u00eb\2\2\u03ac\u03ad\7\u009c\2\2\u03ad\u03b2\5\u00c2b\2\u03ae\u03af"+
		"\7\63\2\2\u03af\u03b1\5\u00c2b\2\u03b0\u03ae\3\2\2\2\u03b1\u03b4\3\2\2"+
		"\2\u03b2\u03b0\3\2\2\2\u03b2\u03b3\3\2\2\2\u03b3\u03b6\3\2\2\2\u03b4\u03b2"+
		"\3\2\2\2\u03b5\u03ac\3\2\2\2\u03b5\u03b6\3\2\2\2\u03b6\u03b9\3\2\2\2\u03b7"+
		"\u03b8\7u\2\2\u03b8\u03ba\7\u00eb\2\2\u03b9\u03b7\3\2\2\2\u03b9\u03ba"+
		"\3\2\2\2\u03ba\u03bc\3\2\2\2\u03bb\u03bd\t\20\2\2\u03bc\u03bb\3\2\2\2"+
		"\u03bc\u03bd\3\2\2\2\u03bd\u03bf\3\2\2\2\u03be\u03c0\t\21\2\2\u03bf\u03be"+
		"\3\2\2\2\u03bf\u03c0\3\2\2\2\u03c0\u03c2\3\2\2\2\u03c1\u03c3\t\17\2\2"+
		"\u03c2\u03c1\3\2\2\2\u03c2\u03c3\3\2\2\2\u03c3\u03c4\3\2\2\2\u03c4\u03c5"+
		"\7\20\2\2\u03c5K\3\2\2\2\u03c6\u03c7\7<\2\2\u03c7\u03c8\5\u00ccg\2\u03c8"+
		"M\3\2\2\2\u03c9\u03ca\7?\2\2\u03ca\u03cb\5\u00ccg\2\u03cbO\3\2\2\2\u03cc"+
		"\u03cd\7g\2\2\u03cd\u03ce\7\17\2\2\u03ce\u03cf\5\n\6\2\u03cf\u03d0\7\63"+
		"\2\2\u03d0\u03da\7\u00eb\2\2\u03d1\u03d2\7\u009c\2\2\u03d2\u03d7\5\u00c2"+
		"b\2\u03d3\u03d4\7\63\2\2\u03d4\u03d6\5\u00c2b\2\u03d5\u03d3\3\2\2\2\u03d6"+
		"\u03d9\3\2\2\2\u03d7\u03d5\3\2\2\2\u03d7\u03d8\3\2\2\2\u03d8\u03db\3\2"+
		"\2\2\u03d9\u03d7\3\2\2\2\u03da\u03d1\3\2\2\2\u03da\u03db\3\2\2\2\u03db"+
		"\u03de\3\2\2\2\u03dc\u03dd\7u\2\2\u03dd\u03df\7\u00eb\2\2\u03de\u03dc"+
		"\3\2\2\2\u03de\u03df\3\2\2\2\u03df\u03e2\3\2\2\2\u03e0\u03e3\t\20\2\2"+
		"\u03e1\u03e3\7h\2\2\u03e2\u03e0\3\2\2\2\u03e2\u03e1\3\2\2\2\u03e2\u03e3"+
		"\3\2\2\2\u03e3\u03e6\3\2\2\2\u03e4\u03e7\t\22\2\2\u03e5\u03e7\7h\2\2\u03e6"+
		"\u03e4\3\2\2\2\u03e6\u03e5\3\2\2\2\u03e6\u03e7\3\2\2\2\u03e7\u03ea\3\2"+
		"\2\2\u03e8\u03eb\t\23\2\2\u03e9\u03eb\7h\2\2\u03ea\u03e8\3\2\2\2\u03ea"+
		"\u03e9\3\2\2\2\u03ea\u03eb\3\2\2\2\u03eb\u03ec\3\2\2\2\u03ec\u03ed\7\20"+
		"\2\2\u03edQ\3\2\2\2\u03ee\u03ef\7#\2\2\u03ef\u03f9\5\u0088E\2\u03f0\u03f9"+
		"\5\u0084C\2\u03f1\u03f9\5\u0086D\2\u03f2\u03f9\5\u008cG\2\u03f3\u03f9"+
		"\5\u0096L\2\u03f4\u03f9\5\u0092J\2\u03f5\u03f9\5\u009cO\2\u03f6\u03f9"+
		"\5\u009aN\2\u03f7\u03f9\5\u009eP\2\u03f8\u03ee\3\2\2\2\u03f8\u03f0\3\2"+
		"\2\2\u03f8\u03f1\3\2\2\2\u03f8\u03f2\3\2\2\2\u03f8\u03f3\3\2\2\2\u03f8"+
		"\u03f4\3\2\2\2\u03f8\u03f5\3\2\2\2\u03f8\u03f6\3\2\2\2\u03f8\u03f7\3\2"+
		"\2\2\u03f9S\3\2\2\2\u03fa\u03fd\5\u00aaV\2\u03fb\u03fd\5z>\2\u03fc\u03fa"+
		"\3\2\2\2\u03fc\u03fb\3\2\2\2\u03fc\u03fd\3\2\2\2\u03fd\u03fe\3\2\2\2\u03fe"+
		"\u0400\7\17\2\2\u03ff\u0401\5\n\6\2\u0400\u03ff\3\2\2\2\u0400\u0401\3"+
		"\2\2\2\u0401\u0406\3\2\2\2\u0402\u0403\7\63\2\2\u0403\u0405\5\n\6\2\u0404"+
		"\u0402\3\2\2\2\u0405\u0408\3\2\2\2\u0406\u0404\3\2\2\2\u0406\u0407\3\2"+
		"\2\2\u0407\u0409\3\2\2\2\u0408\u0406\3\2\2\2\u0409\u040a\7\u008d\2\2\u040a"+
		"\u040c\7\17\2\2\u040b\u040d\5V,\2\u040c\u040b\3\2\2\2\u040c\u040d\3\2"+
		"\2\2\u040d\u040f\3\2\2\2\u040e\u0410\5X-\2\u040f\u040e\3\2\2\2\u040f\u0410"+
		"\3\2\2\2\u0410\u0412\3\2\2\2\u0411\u0413\5Z.\2\u0412\u0411\3\2\2\2\u0412"+
		"\u0413\3\2\2\2\u0413\u0414\3\2\2\2\u0414\u0415\7\20\2\2\u0415\u0416\7"+
		"\20\2\2\u0416U\3\2\2\2\u0417\u0418\7\u0091\2\2\u0418\u0419\7A\2\2\u0419"+
		"\u041e\7\u00eb\2\2\u041a\u041b\7\63\2\2\u041b\u041d\7\u00eb\2\2\u041c"+
		"\u041a\3\2\2\2\u041d\u0420\3\2\2\2\u041e\u041c\3\2\2\2\u041e\u041f\3\2"+
		"\2\2\u041fW\3\2\2\2\u0420\u041e\3\2\2\2\u0421\u0422\7@\2\2\u0422\u0423"+
		"\7A\2\2\u0423\u0425\5\u00c2b\2\u0424\u0426\t\24\2\2\u0425\u0424\3\2\2"+
		"\2\u0425\u0426\3\2\2\2\u0426\u042e\3\2\2\2\u0427\u0428\7\63\2\2\u0428"+
		"\u042a\5\u00c2b\2\u0429\u042b\t\24\2\2\u042a\u0429\3\2\2\2\u042a\u042b"+
		"\3\2\2\2\u042b\u042d\3\2\2\2\u042c\u0427\3\2\2\2\u042d\u0430\3\2\2\2\u042e"+
		"\u042c\3\2\2\2\u042e\u042f\3\2\2\2\u042fY\3\2\2\2\u0430\u042e\3\2\2\2"+
		"\u0431\u0432\7l\2\2\u0432\u0435\7\u00b0\2\2\u0433\u0435\7\u0093\2\2\u0434"+
		"\u0431\3\2\2\2\u0434\u0433\3\2\2\2\u0435\u0436\3\2\2\2\u0436\u0437\7)"+
		"\2\2\u0437\u0438\5\\/\2\u0438\u0439\7%\2\2\u0439\u043a\5\\/\2\u043a[\3"+
		"\2\2\2\u043b\u043c\7\u00e4\2\2\u043c\u0447\7\u008e\2\2\u043d\u043e\7\u00e4"+
		"\2\2\u043e\u0447\7\u008f\2\2\u043f\u0440\7\u0094\2\2\u0440\u0441\7l\2"+
		"\2\u0441\u0447\7\u00b1\2\2\u0442\u0443\7\u0090\2\2\u0443\u0447\7\u008e"+
		"\2\2\u0444\u0445\7\u0090\2\2\u0445\u0447\7\u008f\2\2\u0446\u043b\3\2\2"+
		"\2\u0446\u043d\3\2\2\2\u0446\u043f\3\2\2\2\u0446\u0442\3\2\2\2\u0446\u0444"+
		"\3\2\2\2\u0447]\3\2\2\2\u0448\u0449\5\u00c8e\2\u0449\u044a\7\17\2\2\u044a"+
		"\u044c\5`\61\2\u044b\u044d\5b\62\2\u044c\u044b\3\2\2\2\u044c\u044d\3\2"+
		"\2\2\u044d\u044e\3\2\2\2\u044e\u044f\7\20\2\2\u044f_\3\2\2\2\u0450\u0453"+
		"\5\n\6\2\u0451\u0452\7$\2\2\u0452\u0454\7\u00eb\2\2\u0453\u0451\3\2\2"+
		"\2\u0453\u0454\3\2\2\2\u0454\u045d\3\2\2\2\u0455\u0456\7\63\2\2\u0456"+
		"\u0459\5\n\6\2\u0457\u0458\7$\2\2\u0458\u045a\7\u00eb\2\2\u0459\u0457"+
		"\3\2\2\2\u0459\u045a\3\2\2\2\u045a\u045c\3\2\2\2\u045b\u0455\3\2\2\2\u045c"+
		"\u045f\3\2\2\2\u045d\u045b\3\2\2\2\u045d\u045e\3\2\2\2\u045e\u0469\3\2"+
		"\2\2\u045f\u045d\3\2\2\2\u0460\u0461\7\33\2\2\u0461\u0466\5\u00c2b\2\u0462"+
		"\u0463\7\63\2\2\u0463\u0465\5\u00c2b\2\u0464\u0462\3\2\2\2\u0465\u0468"+
		"\3\2\2\2\u0466\u0464\3\2\2\2\u0466\u0467\3\2\2\2\u0467\u046a\3\2\2\2\u0468"+
		"\u0466\3\2\2\2\u0469\u0460\3\2\2\2\u0469\u046a\3\2\2\2\u046aa\3\2\2\2"+
		"\u046b\u046d\5t;\2\u046c\u046b\3\2\2\2\u046c\u046d\3\2\2\2\u046d\u0471"+
		"\3\2\2\2\u046e\u0472\5d\63\2\u046f\u0472\5x=\2\u0470\u0472\5j\66\2\u0471"+
		"\u046e\3\2\2\2\u0471\u046f\3\2\2\2\u0471\u0470\3\2\2\2\u0471\u0472\3\2"+
		"\2\2\u0472\u0475\3\2\2\2\u0473\u0476\5p9\2\u0474\u0476\5r:\2\u0475\u0473"+
		"\3\2\2\2\u0475\u0474\3\2\2\2\u0475\u0476\3\2\2\2\u0476\u0478\3\2\2\2\u0477"+
		"\u0479\5v<\2\u0478\u0477\3\2\2\2\u0478\u0479\3\2\2\2\u0479c\3\2\2\2\u047a"+
		"\u047c\5\u00b2Z\2\u047b\u047a\3\2\2\2\u047b\u047c\3\2\2\2\u047c\u047d"+
		"\3\2\2\2\u047d\u0482\5f\64\2\u047e\u047f\7\63\2\2\u047f\u0481\5f\64\2"+
		"\u0480\u047e\3\2\2\2\u0481\u0484\3\2\2\2\u0482\u0480\3\2\2\2\u0482\u0483"+
		"\3\2\2\2\u0483e\3\2\2\2\u0484\u0482\3\2\2\2\u0485\u0487\7!\2\2\u0486\u0488"+
		"\5\u00b2Z\2\u0487\u0486\3\2\2\2\u0487\u0488\3\2\2\2\u0488\u0489\3\2\2"+
		"\2\u0489\u0491\5h\65\2\u048a\u048c\7\63\2\2\u048b\u048d\5\u00b2Z\2\u048c"+
		"\u048b\3\2\2\2\u048c\u048d\3\2\2\2\u048d\u048e\3\2\2\2\u048e\u0490\5h"+
		"\65\2\u048f\u048a\3\2\2\2\u0490\u0493\3\2\2\2\u0491\u048f\3\2\2\2\u0491"+
		"\u0492\3\2\2\2\u0492g\3\2\2\2\u0493\u0491\3\2\2\2\u0494\u0495\5\u00c2"+
		"b\2\u0495\u0496\7\25\2\2\u0496\u0497\5\n\6\2\u0497i\3\2\2\2\u0498\u049a"+
		"\5\u00b2Z\2\u0499\u0498\3\2\2\2\u0499\u049a\3\2\2\2\u049a\u049b\3\2\2"+
		"\2\u049b\u049c\7>\2\2\u049c\u04a1\5l\67\2\u049d\u049e\7\63\2\2\u049e\u04a0"+
		"\5l\67\2\u049f\u049d\3\2\2\2\u04a0\u04a3\3\2\2\2\u04a1\u049f\3\2\2\2\u04a1"+
		"\u04a2\3\2\2\2\u04a2\u04a5\3\2\2\2\u04a3\u04a1\3\2\2\2\u04a4\u04a6\5\u00ac"+
		"W\2\u04a5\u04a4\3\2\2\2\u04a5\u04a6\3\2\2\2\u04a6\u04a8\3\2\2\2\u04a7"+
		"\u04a9\5\u00aeX\2\u04a8\u04a7\3\2\2\2\u04a8\u04a9\3\2\2\2\u04a9k\3\2\2"+
		"\2\u04aa\u04ac\5\u00b2Z\2\u04ab\u04aa\3\2\2\2\u04ab\u04ac\3\2\2\2\u04ac"+
		"\u04ad\3\2\2\2\u04ad\u04b2\5n8\2\u04ae\u04af\7\63\2\2\u04af\u04b1\5n8"+
		"\2\u04b0\u04ae\3\2\2\2\u04b1\u04b4\3\2\2\2\u04b2\u04b0\3\2\2\2\u04b2\u04b3"+
		"\3\2\2\2\u04b3m\3\2\2\2\u04b4\u04b2\3\2\2\2\u04b5\u04b6\5\u00c2b\2\u04b6"+
		"\u04b7\7\25\2\2\u04b7\u04b8\5\u008aF\2\u04b8o\3\2\2\2\u04b9\u04ba\7 \2"+
		"\2\u04ba\u04bf\5\u0098M\2\u04bb\u04bc\7\63\2\2\u04bc\u04be\5\u0098M\2"+
		"\u04bd\u04bb\3\2\2\2\u04be\u04c1\3\2\2\2\u04bf\u04bd\3\2\2\2\u04bf\u04c0"+
		"\3\2\2\2\u04c0q\3\2\2\2\u04c1\u04bf\3\2\2\2\u04c2\u04c3\7\37\2\2\u04c3"+
		"\u04c8\5\u0094K\2\u04c4\u04c5\7\63\2\2\u04c5\u04c7\5\u0094K\2\u04c6\u04c4"+
		"\3\2\2\2\u04c7\u04ca\3\2\2\2\u04c8\u04c6\3\2\2\2\u04c8\u04c9\3\2\2\2\u04c9"+
		"s\3\2\2\2\u04ca\u04c8\3\2\2\2\u04cb\u04cc\7_\2\2\u04cc\u04cf\5\n\6\2\u04cd"+
		"\u04cf\5\u00be`\2\u04ce\u04cb\3\2\2\2\u04ce\u04cd\3\2\2\2\u04cfu\3\2\2"+
		"\2\u04d0\u04d4\7#\2\2\u04d1\u04d2\5\u00c2b\2\u04d2\u04d3\7\26\2\2\u04d3"+
		"\u04d5\3\2\2\2\u04d4\u04d1\3\2\2\2\u04d4\u04d5\3\2\2\2\u04d5\u04d6\3\2"+
		"\2\2\u04d6\u04d7\5\u00c0a\2\u04d7\u04db\79\2\2\u04d8\u04d9\5\u00c2b\2"+
		"\u04d9\u04da\7\26\2\2\u04da\u04dc\3\2\2\2\u04db\u04d8\3\2\2\2\u04db\u04dc"+
		"\3\2\2\2\u04dc\u04dd\3\2\2\2\u04dd\u04ef\5\u00c0a\2\u04de\u04e2\7\63\2"+
		"\2\u04df\u04e0\5\u00c2b\2\u04e0\u04e1\7\26\2\2\u04e1\u04e3\3\2\2\2\u04e2"+
		"\u04df\3\2\2\2\u04e2\u04e3\3\2\2\2\u04e3\u04e4\3\2\2\2\u04e4\u04e5\5\u00c0"+
		"a\2\u04e5\u04e9\79\2\2\u04e6\u04e7\5\u00c2b\2\u04e7\u04e8\7\26\2\2\u04e8"+
		"\u04ea\3\2\2\2\u04e9\u04e6\3\2\2\2\u04e9\u04ea\3\2\2\2\u04ea\u04eb\3\2"+
		"\2\2\u04eb\u04ec\5\u00c0a\2\u04ec\u04ee\3\2\2\2\u04ed\u04de\3\2\2\2\u04ee"+
		"\u04f1\3\2\2\2\u04ef\u04f0\3\2\2\2\u04ef\u04ed\3\2\2\2\u04f0w\3\2\2\2"+
		"\u04f1\u04ef\3\2\2\2\u04f2\u04f3\7\u00c1\2\2\u04f3\u04f4\5\n\6\2\u04f4"+
		"y\3\2\2\2\u04f5\u04f6\t\25\2\2\u04f6{\3\2\2\2\u04f7\u04fc\5~@\2\u04f8"+
		"\u04f9\7\63\2\2\u04f9\u04fb\5~@\2\u04fa\u04f8\3\2\2\2\u04fb\u04fe\3\2"+
		"\2\2\u04fc\u04fa\3\2\2\2\u04fc\u04fd\3\2\2\2\u04fd}\3\2\2\2\u04fe\u04fc"+
		"\3\2\2\2\u04ff\u0501\5\u00b2Z\2\u0500\u04ff\3\2\2\2\u0500\u0501\3\2\2"+
		"\2\u0501\u0502\3\2\2\2\u0502\u0503\5\u00c2b\2\u0503\u0504\7\25\2\2\u0504"+
		"\u0505\5\u008aF\2\u0505\177\3\2\2\2\u0506\u050b\5\u0082B\2\u0507\u0508"+
		"\7\63\2\2\u0508\u050a\5\u0082B\2\u0509\u0507\3\2\2\2\u050a\u050d\3\2\2"+
		"\2\u050b\u0509\3\2\2\2\u050b\u050c\3\2\2\2\u050c\u0081\3\2\2\2\u050d\u050b"+
		"\3\2\2\2\u050e\u0510\7_\2\2\u050f\u050e\3\2\2\2\u050f\u0510\3\2\2\2\u0510"+
		"\u0511\3\2\2\2\u0511\u0512\5\n\6\2\u0512\u0083\3\2\2\2\u0513\u0514\7>"+
		"\2\2\u0514\u0516\5|?\2\u0515\u0517\5\u00acW\2\u0516\u0515\3\2\2\2\u0516"+
		"\u0517\3\2\2\2\u0517\u0519\3\2\2\2\u0518\u051a\5\u00aeX\2\u0519\u0518"+
		"\3\2\2\2\u0519\u051a\3\2\2\2\u051a\u0085\3\2\2\2\u051b\u051c\7_\2\2\u051c"+
		"\u051d\5\n\6\2\u051d\u0087\3\2\2\2\u051e\u051f\5\u00c0a\2\u051f\u0520"+
		"\79\2\2\u0520\u0528\5\u00c0a\2\u0521\u0522\7\63\2\2\u0522\u0523\5\u00c0"+
		"a\2\u0523\u0524\79\2\2\u0524\u0525\5\u00c0a\2\u0525\u0527\3\2\2\2\u0526"+
		"\u0521\3\2\2\2\u0527\u052a\3\2\2\2\u0528\u0526\3\2\2\2\u0528\u0529\3\2"+
		"\2\2\u0529\u0089\3\2\2\2\u052a\u0528\3\2\2\2\u052b\u052c\7X\2\2\u052c"+
		"\u052d\7\17\2\2\u052d\u052e\5\n\6\2\u052e\u052f\7\20\2\2\u052f\u0564\3"+
		"\2\2\2\u0530\u0531\7Y\2\2\u0531\u0532\7\17\2\2\u0532\u0533\5\n\6\2\u0533"+
		"\u0534\7\20\2\2\u0534\u0564\3\2\2\2\u0535\u0536\7[\2\2\u0536\u0538\7\17"+
		"\2\2\u0537\u0539\5\n\6\2\u0538\u0537\3\2\2\2\u0538\u0539\3\2\2\2\u0539"+
		"\u053a\3\2\2\2\u053a\u0564\7\20\2\2\u053b\u053c\7Z\2\2\u053c\u053d\7\17"+
		"\2\2\u053d\u053e\5\n\6\2\u053e\u053f\7\20\2\2\u053f\u0564\3\2\2\2\u0540"+
		"\u0541\7E\2\2\u0541\u0542\7\17\2\2\u0542\u0543\5\n\6\2\u0543\u0544\7\20"+
		"\2\2\u0544\u0564\3\2\2\2\u0545\u0546\7F\2\2\u0546\u0547\7\17\2\2\u0547"+
		"\u0548\5\n\6\2\u0548\u0549\7\20\2\2\u0549\u0564\3\2\2\2\u054a\u054b\7"+
		"B\2\2\u054b\u054c\7\17\2\2\u054c\u054d\5\n\6\2\u054d\u054e\7\20\2\2\u054e"+
		"\u0564\3\2\2\2\u054f\u0550\7\u0081\2\2\u0550\u0551\7\17\2\2\u0551\u0552"+
		"\5\n\6\2\u0552\u0553\7\20\2\2\u0553\u0564\3\2\2\2\u0554\u0555\7\u0082"+
		"\2\2\u0555\u0556\7\17\2\2\u0556\u0557\5\n\6\2\u0557\u0558\7\20\2\2\u0558"+
		"\u0564\3\2\2\2\u0559\u055a\7\u0083\2\2\u055a\u055b\7\17\2\2\u055b\u055c"+
		"\5\n\6\2\u055c\u055d\7\20\2\2\u055d\u0564\3\2\2\2\u055e\u055f\7\u0084"+
		"\2\2\u055f\u0560\7\17\2\2\u0560\u0561\5\n\6\2\u0561\u0562\7\20\2\2\u0562"+
		"\u0564\3\2\2\2\u0563\u052b\3\2\2\2\u0563\u0530\3\2\2\2\u0563\u0535\3\2"+
		"\2\2\u0563\u053b\3\2\2\2\u0563\u0540\3\2\2\2\u0563\u0545\3\2\2\2\u0563"+
		"\u054a\3\2\2\2\u0563\u054f\3\2\2\2\u0563\u0554\3\2\2\2\u0563\u0559\3\2"+
		"\2\2\u0563\u055e\3\2\2\2\u0564\u008b\3\2\2\2\u0565\u0566\7!\2\2\u0566"+
		"\u056b\5\u008eH\2\u0567\u0568\7\63\2\2\u0568\u056a\5\u008eH\2\u0569\u0567"+
		"\3\2\2\2\u056a\u056d\3\2\2\2\u056b\u0569\3\2\2\2\u056b\u056c\3\2\2\2\u056c"+
		"\u008d\3\2\2\2\u056d\u056b\3\2\2\2\u056e\u0570\5\u00b2Z\2\u056f\u056e"+
		"\3\2\2\2\u056f\u0570\3\2\2\2\u0570\u0571\3\2\2\2\u0571\u0572\5\u00c2b"+
		"\2\u0572\u0573\7\25\2\2\u0573\u0574\5\u0090I\2\u0574\u008f\3\2\2\2\u0575"+
		"\u0576\5\u008aF\2\u0576\u0577\7\17\2\2\u0577\u0578\5\n\6\2\u0578\u0579"+
		"\7\20\2\2\u0579\u057c\3\2\2\2\u057a\u057c\5\n\6\2\u057b\u0575\3\2\2\2"+
		"\u057b\u057a\3\2\2\2\u057c\u0091\3\2\2\2\u057d\u057e\7\37\2\2\u057e\u0583"+
		"\5\u0094K\2\u057f\u0580\7\63\2\2\u0580\u0582\5\u0094K\2\u0581\u057f\3"+
		"\2\2\2\u0582\u0585\3\2\2\2\u0583\u0581\3\2\2\2\u0583\u0584\3\2\2\2\u0584"+
		"\u0093\3\2\2\2\u0585\u0583\3\2\2\2\u0586\u058c\5\u00c2b\2\u0587\u0588"+
		"\5\u00bc_\2\u0588\u0589\7\26\2\2\u0589\u058a\5\u00c2b\2\u058a\u058c\3"+
		"\2\2\2\u058b\u0586\3\2\2\2\u058b\u0587\3\2\2\2\u058c\u0095\3\2\2\2\u058d"+
		"\u058e\7 \2\2\u058e\u0593\5\u0098M\2\u058f\u0590\7\63\2\2\u0590\u0592"+
		"\5\u0098M\2\u0591\u058f\3\2\2\2\u0592\u0595\3\2\2\2\u0593\u0591\3\2\2"+
		"\2\u0593\u0594\3\2\2\2\u0594\u0097\3\2\2\2\u0595\u0593\3\2\2\2\u0596\u059c"+
		"\5\u00c2b\2\u0597\u0598\5\u00bc_\2\u0598\u0599\7\26\2\2\u0599\u059a\5"+
		"\u00c2b\2\u059a\u059c\3\2\2\2\u059b\u0596\3\2\2\2\u059b\u0597\3\2\2\2"+
		"\u059c\u0099\3\2\2\2\u059d\u059e\7\u00bf\2\2\u059e\u059f\5\u00c0a\2\u059f"+
		"\u05a0\7\63\2\2\u05a0\u05a1\5\u00c0a\2\u05a1\u009b\3\2\2\2\u05a2\u05a3"+
		"\7\u00be\2\2\u05a3\u05a4\5\u00c0a\2\u05a4\u05a5\7\63\2\2\u05a5\u05a6\5"+
		"\u00c0a\2\u05a6\u009d\3\2\2\2\u05a7\u05a8\7\u00c0\2\2\u05a8\u05a9\5\u00c0"+
		"a\2\u05a9\u05aa\7\r\2\2\u05aa\u05b2\5\u00ccg\2\u05ab\u05ac\7\63\2\2\u05ac"+
		"\u05ad\5\u00c0a\2\u05ad\u05ae\7\r\2\2\u05ae\u05af\5\u00ccg\2\u05af\u05b1"+
		"\3\2\2\2\u05b0\u05ab\3\2\2\2\u05b1\u05b4\3\2\2\2\u05b2\u05b0\3\2\2\2\u05b2"+
		"\u05b3\3\2\2\2\u05b3\u009f\3\2\2\2\u05b4\u05b2\3\2\2\2\u05b5\u05b8\7*"+
		"\2\2\u05b6\u05b9\5\u00a2R\2\u05b7\u05b9\7\u00eb\2\2\u05b8\u05b6\3\2\2"+
		"\2\u05b8\u05b7\3\2\2\2\u05b9\u05c0\3\2\2\2\u05ba\u05bd\7+\2\2\u05bb\u05be"+
		"\5\u00a2R\2\u05bc\u05be\7\u00eb\2\2\u05bd\u05bb\3\2\2\2\u05bd\u05bc\3"+
		"\2\2\2\u05be\u05c0\3\2\2\2\u05bf\u05b5\3\2\2\2\u05bf\u05ba\3\2\2\2\u05c0"+
		"\u00a1\3\2\2\2\u05c1\u05c2\7.\2\2\u05c2\u05c3\7\17\2\2\u05c3\u05c8\5\n"+
		"\6\2\u05c4\u05c5\7\63\2\2\u05c5\u05c7\5\n\6\2\u05c6\u05c4\3\2\2\2\u05c7"+
		"\u05ca\3\2\2\2\u05c8\u05c6\3\2\2\2\u05c8\u05c9\3\2\2\2\u05c9\u05cb\3\2"+
		"\2\2\u05ca\u05c8\3\2\2\2\u05cb\u05cc\7\20\2\2\u05cc\u05e8\3\2\2\2\u05cd"+
		"\u05ce\7\60\2\2\u05ce\u05cf\7\17\2\2\u05cf\u05d0\5\n\6\2\u05d0\u05d1\7"+
		"\63\2\2\u05d1\u05d2\5\n\6\2\u05d2\u05d3\7\20\2\2\u05d3\u05e8\3\2\2\2\u05d4"+
		"\u05d5\7\u0080\2\2\u05d5\u05d6\7\17\2\2\u05d6\u05d7\5\n\6\2\u05d7\u05d8"+
		"\7\63\2\2\u05d8\u05d9\5\n\6\2\u05d9\u05da\7\20\2\2\u05da\u05e8\3\2\2\2"+
		"\u05db\u05dc\7\61\2\2\u05dc\u05dd\7\17\2\2\u05dd\u05e2\5\n\6\2\u05de\u05df"+
		"\7\63\2\2\u05df\u05e1\5\n\6\2\u05e0\u05de\3\2\2\2\u05e1\u05e4\3\2\2\2"+
		"\u05e2\u05e0\3\2\2\2\u05e2\u05e3\3\2\2\2\u05e3\u05e5\3\2\2\2\u05e4\u05e2"+
		"\3\2\2\2\u05e5\u05e6\7\20\2\2\u05e6\u05e8\3\2\2\2\u05e7\u05c1\3\2\2\2"+
		"\u05e7\u05cd\3\2\2\2\u05e7\u05d4\3\2\2\2\u05e7\u05db\3\2\2\2\u05e8\u00a3"+
		"\3\2\2\2\u05e9\u05ea\5\u00ba^\2\u05ea\u05eb\7\3\2\2\u05eb\u05ec\5\u00c2"+
		"b\2\u05ec\u05ed\7\r\2\2\u05ed\u05f3\5\u00ccg\2\u05ee\u05ef\7\63\2\2\u05ef"+
		"\u05f0\5\u00c2b\2\u05f0\u05f1\7\r\2\2\u05f1\u05f2\5\u00ccg\2\u05f2\u05f4"+
		"\3\2\2\2\u05f3\u05ee\3\2\2\2\u05f3\u05f4\3\2\2\2\u05f4\u05f5\3\2\2\2\u05f5"+
		"\u05f6\7\4\2\2\u05f6\u00a5\3\2\2\2\u05f7\u05f8\5\u00aaV\2\u05f8\u05f9"+
		"\7\17\2\2\u05f9\u05fc\7\u00eb\2\2\u05fa\u05fb\7\26\2\2\u05fb\u05fd\5\u00c2"+
		"b\2\u05fc\u05fa\3\2\2\2\u05fc\u05fd\3\2\2\2\u05fd\u0606\3\2\2\2\u05fe"+
		"\u05ff\7\63\2\2\u05ff\u0602\7\u00eb\2\2\u0600\u0601\7\26\2\2\u0601\u0603"+
		"\5\u00c2b\2\u0602\u0600\3\2\2\2\u0602\u0603\3\2\2\2\u0603\u0605\3\2\2"+
		"\2\u0604\u05fe\3\2\2\2\u0605\u0608\3\2\2\2\u0606\u0604\3\2\2\2\u0606\u0607"+
		"\3\2\2\2\u0607\u060a\3\2\2\2\u0608\u0606\3\2\2\2\u0609\u060b\5\u00acW"+
		"\2\u060a\u0609\3\2\2\2\u060a\u060b\3\2\2\2\u060b\u060d\3\2\2\2\u060c\u060e"+
		"\5\u00aeX\2\u060d\u060c\3\2\2\2\u060d\u060e\3\2\2\2\u060e\u060f\3\2\2"+
		"\2\u060f\u0610\7\20\2\2\u0610\u00a7\3\2\2\2\u0611\u0612\5\u00aaV\2\u0612"+
		"\u0613\7\17\2\2\u0613\u0616\7\u00eb\2\2\u0614\u0615\7\26\2\2\u0615\u0617"+
		"\5\u00c2b\2\u0616\u0614\3\2\2\2\u0616\u0617\3\2\2\2\u0617\u0620\3\2\2"+
		"\2\u0618\u0619\7\63\2\2\u0619\u061c\7\u00eb\2\2\u061a\u061b\7\26\2\2\u061b"+
		"\u061d\5\u00c2b\2\u061c\u061a\3\2\2\2\u061c\u061d\3\2\2\2\u061d\u061f"+
		"\3\2\2\2\u061e\u0618\3\2\2\2\u061f\u0622\3\2\2\2\u0620\u061e\3\2\2\2\u0620"+
		"\u0621\3\2\2\2\u0621\u0623\3\2\2\2\u0622\u0620\3\2\2\2\u0623\u0625\7\20"+
		"\2\2\u0624\u0626\5\u00acW\2\u0625\u0624\3\2\2\2\u0625\u0626\3\2\2\2\u0626"+
		"\u0628\3\2\2\2\u0627\u0629\5\u00aeX\2\u0628\u0627\3\2\2\2\u0628\u0629"+
		"\3\2\2\2\u0629\u00a9\3\2\2\2\u062a\u062b\t\26\2\2\u062b\u00ab\3\2\2\2"+
		"\u062c\u0636\5\u00caf\2\u062d\u0632\7\u00eb\2\2\u062e\u062f\7\63\2\2\u062f"+
		"\u0631\7\u00eb\2\2\u0630\u062e\3\2\2\2\u0631\u0634\3\2\2\2\u0632\u0630"+
		"\3\2\2\2\u0632\u0633\3\2\2\2\u0633\u0637\3\2\2\2\u0634\u0632\3\2\2\2\u0635"+
		"\u0637\5\n\6\2\u0636\u062d\3\2\2\2\u0636\u0635\3\2\2\2\u0637\u00ad\3\2"+
		"\2\2\u0638\u063a\7\u0087\2\2\u0639\u063b\7\17\2\2\u063a\u0639\3\2\2\2"+
		"\u063a\u063b\3\2\2\2\u063b\u063d\3\2\2\2\u063c\u063e\5\u008aF\2\u063d"+
		"\u063c\3\2\2\2\u063d\u063e\3\2\2\2\u063e\u063f\3\2\2\2\u063f\u0641\5\n"+
		"\6\2\u0640\u0642\7\20\2\2\u0641\u0640\3\2\2\2\u0641\u0642\3\2\2\2\u0642"+
		"\u00af\3\2\2\2\u0643\u0644\7:\2\2\u0644\u0645\7=\2\2\u0645\u0646\7l\2"+
		"\2\u0646\u0647\7\u00b0\2\2\u0647\u00b1\3\2\2\2\u0648\u064e\7]\2\2\u0649"+
		"\u064e\7\u00dd\2\2\u064a\u064e\7\\\2\2\u064b\u064e\7^\2\2\u064c\u064e"+
		"\5\u00b4[\2\u064d\u0648\3\2\2\2\u064d\u0649\3\2\2\2\u064d\u064a\3\2\2"+
		"\2\u064d\u064b\3\2\2\2\u064d\u064c\3\2\2\2\u064e\u00b3\3\2\2\2\u064f\u0650"+
		"\7c\2\2\u0650\u0651\7^\2\2\u0651\u00b5\3\2\2\2\u0652\u0653\5\n\6\2\u0653"+
		"\u00b7\3\2\2\2\u0654\u0655\t\27\2\2\u0655\u00b9\3\2\2\2\u0656\u0657\7"+
		"\u00ea\2\2\u0657\u00bb\3\2\2\2\u0658\u0659\7\u00eb\2\2\u0659\u00bd\3\2"+
		"\2\2\u065a\u065b\7\u00eb\2\2\u065b\u00bf\3\2\2\2\u065c\u065d\7\u00eb\2"+
		"\2\u065d\u00c1\3\2\2\2\u065e\u065f\7\u00eb\2\2\u065f\u00c3\3\2\2\2\u0660"+
		"\u0661\7\u00eb\2\2\u0661\u00c5\3\2\2\2\u0662\u0663\7\u00eb\2\2\u0663\u00c7"+
		"\3\2\2\2\u0664\u0665\t\30\2\2\u0665\u00c9\3\2\2\2\u0666\u0667\7\u0085"+
		"\2\2\u0667\u066d\7A\2\2\u0668\u0669\7\u0085\2\2\u0669\u066d\7\u0086\2"+
		"\2\u066a\u066b\7\u0085\2\2\u066b\u066d\7=\2\2\u066c\u0666\3\2\2\2\u066c"+
		"\u0668\3\2\2\2\u066c\u066a\3\2\2\2\u066d\u00cb\3\2\2\2\u066e\u066f\t\31"+
		"\2\2\u066f\u00cd\3\2\2\2\u0670\u0671\t\32\2\2\u0671\u00cf\3\2\2\2\u0672"+
		"\u0676\5\u00d2j\2\u0673\u0676\5\u00d4k\2\u0674\u0676\5\u00d6l\2\u0675"+
		"\u0672\3\2\2\2\u0675\u0673\3\2\2\2\u0675\u0674\3\2\2\2\u0676\u0678\3\2"+
		"\2\2\u0677\u0679\5\u00d8m\2\u0678\u0677\3\2\2\2\u0678\u0679\3\2\2\2\u0679"+
		"\u067e\3\2\2\2\u067a\u067c\7(\2\2\u067b\u067a\3\2\2\2\u067b\u067c\3\2"+
		"\2\2\u067c\u067d\3\2\2\2\u067d\u067f\7\u00e9\2\2\u067e\u067b\3\2\2\2\u067e"+
		"\u067f\3\2\2\2\u067f\u00d1\3\2\2\2\u0680\u0681\t\33\2\2\u0681\u00d3\3"+
		"\2\2\2\u0682\u0683\7\u00eb\2\2\u0683\u00d5\3\2\2\2\u0684\u0685\7\u00eb"+
		"\2\2\u0685\u00d7\3\2\2\2\u0686\u0687\7\3\2\2\u0687\u0688\5\n\6\2\u0688"+
		"\u0689\7\4\2\2\u0689\u0696\3\2\2\2\u068a\u068b\7\22\2\2\u068b\u0690\5"+
		"\u00ccg\2\u068c\u068d\7\63\2\2\u068d\u068f\5\u00ccg\2\u068e\u068c\3\2"+
		"\2\2\u068f\u0692\3\2\2\2\u0690\u068e\3\2\2\2\u0690\u0691\3\2\2\2\u0691"+
		"\u0693\3\2\2\2\u0692\u0690\3\2\2\2\u0693\u0694\7\23\2\2\u0694\u0696\3"+
		"\2\2\2\u0695\u0686\3\2\2\2\u0695\u068a\3\2\2\2\u0696\u00d9\3\2\2\2\u0697"+
		"\u069e\5\u00d0i\2\u0698\u069e\5\u00dco\2\u0699\u069e\5\u00dep\2\u069a"+
		"\u069e\5\u00f4{\2\u069b\u069e\5\u00eex\2\u069c\u069e\5\u00e4s\2\u069d"+
		"\u0697\3\2\2\2\u069d\u0698\3\2\2\2\u069d\u0699\3\2\2\2\u069d\u069a\3\2"+
		"\2\2\u069d\u069b\3\2\2\2\u069d\u069c\3\2\2\2\u069e\u00db\3\2\2\2\u069f"+
		"\u06a4\5\u00b2Z\2\u06a0\u06a1\7\n\2\2\u06a1\u06a2\5\u00d0i\2\u06a2\u06a3"+
		"\7\t\2\2\u06a3\u06a5\3\2\2\2\u06a4\u06a0\3\2\2\2\u06a4\u06a5\3\2\2\2\u06a5"+
		"\u00dd\3\2\2\2\u06a6\u06b2\7n\2\2\u06a7\u06a8\7\22\2\2\u06a8\u06ad\5\u00e0"+
		"q\2\u06a9\u06aa\7\63\2\2\u06aa\u06ac\5\u00e0q\2\u06ab\u06a9\3\2\2\2\u06ac"+
		"\u06af\3\2\2\2\u06ad\u06ab\3\2\2\2\u06ad\u06ae\3\2\2\2\u06ae\u06b0\3\2"+
		"\2\2\u06af\u06ad\3\2\2\2\u06b0\u06b1\7\23\2\2\u06b1\u06b3\3\2\2\2\u06b2"+
		"\u06a7\3\2\2\2\u06b2\u06b3\3\2\2\2\u06b3\u00df\3\2\2\2\u06b4\u06b7\5\u00dc"+
		"o\2\u06b5\u06b8\5\u00c2b\2\u06b6\u06b8\5\u00e2r\2\u06b7\u06b5\3\2\2\2"+
		"\u06b7\u06b6\3\2\2\2\u06b8\u00e1\3\2\2\2\u06b9\u06bb\7h\2\2\u06ba\u06bc"+
		"\t\34\2\2\u06bb\u06ba\3\2\2\2\u06bb\u06bc\3\2\2\2\u06bc\u00e3\3\2\2\2"+
		"\u06bd\u06c1\7t\2\2\u06be\u06c1\5\u00e6t\2\u06bf\u06c1\5\u00e8u\2\u06c0"+
		"\u06bd\3\2\2\2\u06c0\u06be\3\2\2\2\u06c0\u06bf\3\2\2\2\u06c1\u00e5\3\2"+
		"\2\2\u06c2\u06ce\7r\2\2\u06c3\u06c4\7\u00de\2\2\u06c4\u06c5\7\22\2\2\u06c5"+
		"\u06c6\5\u00eav\2\u06c6\u06c7\7\23\2\2\u06c7\u06ce\3\2\2\2\u06c8\u06c9"+
		"\7\u00df\2\2\u06c9\u06ca\7\22\2\2\u06ca\u06cb\5\u00ecw\2\u06cb\u06cc\7"+
		"\23\2\2\u06cc\u06ce\3\2\2\2\u06cd\u06c2\3\2\2\2\u06cd\u06c3\3\2\2\2\u06cd"+
		"\u06c8\3\2\2\2\u06ce\u00e7\3\2\2\2\u06cf\u06ea\7s\2\2\u06d0\u06da\7\u00e0"+
		"\2\2\u06d1\u06d2\7\22\2\2\u06d2\u06d7\7\u00eb\2\2\u06d3\u06d4\7\17\2\2"+
		"\u06d4\u06d5\5\u00eav\2\u06d5\u06d6\7\20\2\2\u06d6\u06d8\3\2\2\2\u06d7"+
		"\u06d3\3\2\2\2\u06d7\u06d8\3\2\2\2\u06d8\u06d9\3\2\2\2\u06d9\u06db\7\23"+
		"\2\2\u06da\u06d1\3\2\2\2\u06da\u06db\3\2\2\2\u06db\u06ea\3\2\2\2\u06dc"+
		"\u06e7\7\u00e1\2\2\u06dd\u06de\7\22\2\2\u06de\u06e3\5\u00c0a\2\u06df\u06e0"+
		"\7\17\2\2\u06e0\u06e1\5\u00ecw\2\u06e1\u06e2\7\20\2\2\u06e2\u06e4\3\2"+
		"\2\2\u06e3\u06df\3\2\2\2\u06e3\u06e4\3\2\2\2\u06e4\u06e5\3\2\2\2\u06e5"+
		"\u06e6\7\23\2\2\u06e6\u06e8\3\2\2\2\u06e7\u06dd\3\2\2\2\u06e7\u06e8\3"+
		"\2\2\2\u06e8\u06ea\3\2\2\2\u06e9\u06cf\3\2\2\2\u06e9\u06d0\3\2\2\2\u06e9"+
		"\u06dc\3\2\2\2\u06ea\u00e9\3\2\2\2\u06eb\u06f0\7\u00eb\2\2\u06ec\u06ed"+
		"\7\7\2\2\u06ed\u06ef\7\u00eb\2\2\u06ee\u06ec\3\2\2\2\u06ef\u06f2\3\2\2"+
		"\2\u06f0\u06ee\3\2\2\2\u06f0\u06f1\3\2\2\2\u06f1\u00eb\3\2\2\2\u06f2\u06f0"+
		"\3\2\2\2\u06f3\u06f8\5\u00c0a\2\u06f4\u06f5\7\7\2\2\u06f5\u06f7\5\u00c0"+
		"a\2\u06f6\u06f4\3\2\2\2\u06f7\u06fa\3\2\2\2\u06f8\u06f6\3\2\2\2\u06f8"+
		"\u06f9\3\2\2\2\u06f9\u00ed\3\2\2\2\u06fa\u06f8\3\2\2\2\u06fb\u0700\5\u00f0"+
		"y\2\u06fc\u06fd\7\7\2\2\u06fd\u06ff\5\u00f0y\2\u06fe\u06fc\3\2\2\2\u06ff"+
		"\u0702\3\2\2\2\u0700\u06fe\3\2\2\2\u0700\u0701\3\2\2\2\u0701\u0703\3\2"+
		"\2\2\u0702\u0700\3\2\2\2\u0703\u0704\7\24\2\2\u0704\u0705\5\u00f2z\2\u0705"+
		"\u00ef\3\2\2\2\u0706\u070a\5\u00d0i\2\u0707\u070a\5\u00dep\2\u0708\u070a"+
		"\5\u00dco\2\u0709\u0706\3\2\2\2\u0709\u0707\3\2\2\2\u0709\u0708\3\2\2"+
		"\2\u070a\u00f1\3\2\2\2\u070b\u0711\5\u00d0i\2\u070c\u0711\5\u00dep\2\u070d"+
		"\u0711\5\u00f4{\2\u070e\u0711\5\u00e4s\2\u070f\u0711\5\u00dco\2\u0710"+
		"\u070b\3\2\2\2\u0710\u070c\3\2\2\2\u0710\u070d\3\2\2\2\u0710\u070e\3\2"+
		"\2\2\u0710\u070f\3\2\2\2\u0711\u00f3\3\2\2\2\u0712\u0717\7\u00e2\2\2\u0713"+
		"\u0714\7\n\2\2\u0714\u0715\5\u00d0i\2\u0715\u0716\7\t\2\2\u0716\u0718"+
		"\3\2\2\2\u0717\u0713\3\2\2\2\u0717\u0718\3\2\2\2\u0718\u00f5\3\2\2\2\u0719"+
		"\u071a\t\35\2\2\u071a\u00f7\3\2\2\2\u071b\u071c\7p\2\2\u071c\u071d\7r"+
		"\2\2\u071d\u071e\7t\2\2\u071e\u00f9\3\2\2\2\u071f\u0720\7p\2\2\u0720\u0721"+
		"\7s\2\2\u0721\u0722\7t\2\2\u0722\u00fb\3\2\2\2\u0723\u0724\7v\2\2\u0724"+
		"\u0725\7r\2\2\u0725\u0726\7t\2\2\u0726\u00fd\3\2\2\2\u0727\u0728\7v\2"+
		"\2\u0728\u0729\7s\2\2\u0729\u072a\7t\2\2\u072a\u00ff\3\2\2\2\u072b\u072c"+
		"\7p\2\2\u072c\u072d\7l\2\2\u072d\u072e\7m\2\2\u072e\u0101\3\2\2\2\u00cb"+
		"\u0103\u0108\u010e\u0114\u0118\u011d\u0122\u0126\u012e\u0138\u013e\u0143"+
		"\u014e\u015d\u0178\u017e\u0180\u0182\u018f\u0195\u019a\u01aa\u01af\u01b3"+
		"\u01b6\u01ba\u01c3\u01c9\u01cd\u01d4\u01db\u01df\u01e6\u01ea\u01fa\u01ff"+
		"\u0205\u0209\u020c\u0214\u021a\u021f\u022a\u022d\u0232\u023d\u0243\u0248"+
		"\u024c\u024f\u0258\u0281\u02bd\u02c1\u02cc\u02d0\u02db\u030a\u0312\u031a"+
		"\u0325\u0330\u0335\u0339\u033f\u0343\u034d\u0351\u0358\u0368\u0371\u0376"+
		"\u0378\u037c\u0384\u0387\u038b\u038e\u039d\u03a0\u03a3\u03b2\u03b5\u03b9"+
		"\u03bc\u03bf\u03c2\u03d7\u03da\u03de\u03e2\u03e6\u03ea\u03f8\u03fc\u0400"+
		"\u0406\u040c\u040f\u0412\u041e\u0425\u042a\u042e\u0434\u0446\u044c\u0453"+
		"\u0459\u045d\u0466\u0469\u046c\u0471\u0475\u0478\u047b\u0482\u0487\u048c"+
		"\u0491\u0499\u04a1\u04a5\u04a8\u04ab\u04b2\u04bf\u04c8\u04ce\u04d4\u04db"+
		"\u04e2\u04e9\u04ef\u04fc\u0500\u050b\u050f\u0516\u0519\u0528\u0538\u0563"+
		"\u056b\u056f\u057b\u0583\u058b\u0593\u059b\u05b2\u05b8\u05bd\u05bf\u05c8"+
		"\u05e2\u05e7\u05f3\u05fc\u0602\u0606\u060a\u060d\u0616\u061c\u0620\u0625"+
		"\u0628\u0632\u0636\u063a\u063d\u0641\u064d\u066c\u0675\u0678\u067b\u067e"+
		"\u0690\u0695\u069d\u06a4\u06ad\u06b2\u06b7\u06bb\u06c0\u06cd\u06d7\u06da"+
		"\u06e3\u06e7\u06e9\u06f0\u06f8\u0700\u0709\u0710\u0717";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}