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
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link VtlParser}.
 * 
 * @author Neil Mackenzie
 */
public interface VtlListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link VtlParser#start}.
	 * @param ctx the parse tree
	 */
	void enterStart(VtlParser.StartContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#start}.
	 * @param ctx the parse tree
	 */
	void exitStart(VtlParser.StartContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(VtlParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(VtlParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#persistentAssignment}.
	 * @param ctx the parse tree
	 */
	void enterPersistentAssignment(VtlParser.PersistentAssignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#persistentAssignment}.
	 * @param ctx the parse tree
	 */
	void exitPersistentAssignment(VtlParser.PersistentAssignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#optionalExpr}.
	 * @param ctx the parse tree
	 */
	void enterOptionalExpr(VtlParser.OptionalExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#optionalExpr}.
	 * @param ctx the parse tree
	 */
	void exitOptionalExpr(VtlParser.OptionalExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExpr(VtlParser.ExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExpr(VtlParser.ExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exprValidationExpr}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterExprValidationExpr(VtlParser.ExprValidationExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exprValidationExpr}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitExprValidationExpr(VtlParser.ExprValidationExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code definitionExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterDefinitionExpressions(VtlParser.DefinitionExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code definitionExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitDefinitionExpressions(VtlParser.DefinitionExpressionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code standaloneAggregateFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterStandaloneAggregateFunction(VtlParser.StandaloneAggregateFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code standaloneAggregateFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitStandaloneAggregateFunction(VtlParser.StandaloneAggregateFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code componentExpressionwithAggrClause}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterComponentExpressionwithAggrClause(VtlParser.ComponentExpressionwithAggrClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code componentExpressionwithAggrClause}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitComponentExpressionwithAggrClause(VtlParser.ComponentExpressionwithAggrClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code standaloneAnalyticFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterStandaloneAnalyticFunction(VtlParser.StandaloneAnalyticFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code standaloneAnalyticFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitStandaloneAnalyticFunction(VtlParser.StandaloneAnalyticFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleaggregateFunctions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterSimpleaggregateFunctions(VtlParser.SimpleaggregateFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleaggregateFunctions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitSimpleaggregateFunctions(VtlParser.SimpleaggregateFunctionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code timeexpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterTimeexpressions(VtlParser.TimeexpressionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code timeexpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitTimeexpressions(VtlParser.TimeexpressionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterSetExpressions(VtlParser.SetExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitSetExpressions(VtlParser.SetExpressionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code callFunctionExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterCallFunctionExpression(VtlParser.CallFunctionExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code callFunctionExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitCallFunctionExpression(VtlParser.CallFunctionExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code joinExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void enterJoinExpression(VtlParser.JoinExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code joinExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 */
	void exitJoinExpression(VtlParser.JoinExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#timeExpr}.
	 * @param ctx the parse tree
	 */
	void enterTimeExpr(VtlParser.TimeExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#timeExpr}.
	 * @param ctx the parse tree
	 */
	void exitTimeExpr(VtlParser.TimeExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defHierarchical}.
	 * @param ctx the parse tree
	 */
	void enterDefHierarchical(VtlParser.DefHierarchicalContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defHierarchical}.
	 * @param ctx the parse tree
	 */
	void exitDefHierarchical(VtlParser.DefHierarchicalContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#ruleClauseHierarchical}.
	 * @param ctx the parse tree
	 */
	void enterRuleClauseHierarchical(VtlParser.RuleClauseHierarchicalContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#ruleClauseHierarchical}.
	 * @param ctx the parse tree
	 */
	void exitRuleClauseHierarchical(VtlParser.RuleClauseHierarchicalContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#ruleItemHierarchical}.
	 * @param ctx the parse tree
	 */
	void enterRuleItemHierarchical(VtlParser.RuleItemHierarchicalContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#ruleItemHierarchical}.
	 * @param ctx the parse tree
	 */
	void exitRuleItemHierarchical(VtlParser.RuleItemHierarchicalContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#hierRuleSignature}.
	 * @param ctx the parse tree
	 */
	void enterHierRuleSignature(VtlParser.HierRuleSignatureContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#hierRuleSignature}.
	 * @param ctx the parse tree
	 */
	void exitHierRuleSignature(VtlParser.HierRuleSignatureContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#valueDomainSignature}.
	 * @param ctx the parse tree
	 */
	void enterValueDomainSignature(VtlParser.ValueDomainSignatureContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#valueDomainSignature}.
	 * @param ctx the parse tree
	 */
	void exitValueDomainSignature(VtlParser.ValueDomainSignatureContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#codeItemRelation}.
	 * @param ctx the parse tree
	 */
	void enterCodeItemRelation(VtlParser.CodeItemRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#codeItemRelation}.
	 * @param ctx the parse tree
	 */
	void exitCodeItemRelation(VtlParser.CodeItemRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#codeItemRelationClause}.
	 * @param ctx the parse tree
	 */
	void enterCodeItemRelationClause(VtlParser.CodeItemRelationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#codeItemRelationClause}.
	 * @param ctx the parse tree
	 */
	void exitCodeItemRelationClause(VtlParser.CodeItemRelationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#codeItemRef}.
	 * @param ctx the parse tree
	 */
	void enterCodeItemRef(VtlParser.CodeItemRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#codeItemRef}.
	 * @param ctx the parse tree
	 */
	void exitCodeItemRef(VtlParser.CodeItemRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defDatapoint}.
	 * @param ctx the parse tree
	 */
	void enterDefDatapoint(VtlParser.DefDatapointContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defDatapoint}.
	 * @param ctx the parse tree
	 */
	void exitDefDatapoint(VtlParser.DefDatapointContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#ruleClauseDatapoint}.
	 * @param ctx the parse tree
	 */
	void enterRuleClauseDatapoint(VtlParser.RuleClauseDatapointContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#ruleClauseDatapoint}.
	 * @param ctx the parse tree
	 */
	void exitRuleClauseDatapoint(VtlParser.RuleClauseDatapointContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#ruleItemDatapoint}.
	 * @param ctx the parse tree
	 */
	void enterRuleItemDatapoint(VtlParser.RuleItemDatapointContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#ruleItemDatapoint}.
	 * @param ctx the parse tree
	 */
	void exitRuleItemDatapoint(VtlParser.RuleItemDatapointContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#rulesetSignature}.
	 * @param ctx the parse tree
	 */
	void enterRulesetSignature(VtlParser.RulesetSignatureContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#rulesetSignature}.
	 * @param ctx the parse tree
	 */
	void exitRulesetSignature(VtlParser.RulesetSignatureContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#varSignature}.
	 * @param ctx the parse tree
	 */
	void enterVarSignature(VtlParser.VarSignatureContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#varSignature}.
	 * @param ctx the parse tree
	 */
	void exitVarSignature(VtlParser.VarSignatureContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defExpr}.
	 * @param ctx the parse tree
	 */
	void enterDefExpr(VtlParser.DefExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defExpr}.
	 * @param ctx the parse tree
	 */
	void exitDefExpr(VtlParser.DefExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defOperator}.
	 * @param ctx the parse tree
	 */
	void enterDefOperator(VtlParser.DefOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defOperator}.
	 * @param ctx the parse tree
	 */
	void exitDefOperator(VtlParser.DefOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#parameterItem}.
	 * @param ctx the parse tree
	 */
	void enterParameterItem(VtlParser.ParameterItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#parameterItem}.
	 * @param ctx the parse tree
	 */
	void exitParameterItem(VtlParser.ParameterItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#callFunction}.
	 * @param ctx the parse tree
	 */
	void enterCallFunction(VtlParser.CallFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#callFunction}.
	 * @param ctx the parse tree
	 */
	void exitCallFunction(VtlParser.CallFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code roundAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterRoundAtom(VtlParser.RoundAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code roundAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitRoundAtom(VtlParser.RoundAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ceilAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterCeilAtom(VtlParser.CeilAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ceilAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitCeilAtom(VtlParser.CeilAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code floorAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterFloorAtom(VtlParser.FloorAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code floorAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitFloorAtom(VtlParser.FloorAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code minAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterMinAtom(VtlParser.MinAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code minAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitMinAtom(VtlParser.MinAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code expAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterExpAtom(VtlParser.ExpAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code expAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitExpAtom(VtlParser.ExpAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lnAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterLnAtom(VtlParser.LnAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lnAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitLnAtom(VtlParser.LnAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterLogAtom(VtlParser.LogAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitLogAtom(VtlParser.LogAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code powerAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterPowerAtom(VtlParser.PowerAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code powerAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitPowerAtom(VtlParser.PowerAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sqrtAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterSqrtAtom(VtlParser.SqrtAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sqrtAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitSqrtAtom(VtlParser.SqrtAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterLenAtom(VtlParser.LenAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitLenAtom(VtlParser.LenAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code betweenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterBetweenAtom(VtlParser.BetweenAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code betweenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitBetweenAtom(VtlParser.BetweenAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code trimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterTrimAtom(VtlParser.TrimAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code trimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitTrimAtom(VtlParser.TrimAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ltrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterLtrimAtom(VtlParser.LtrimAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ltrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitLtrimAtom(VtlParser.LtrimAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rtrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterRtrimAtom(VtlParser.RtrimAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rtrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitRtrimAtom(VtlParser.RtrimAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ucaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterUcaseAtom(VtlParser.UcaseAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ucaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitUcaseAtom(VtlParser.UcaseAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lcaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterLcaseAtom(VtlParser.LcaseAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lcaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitLcaseAtom(VtlParser.LcaseAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code substrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterSubstrAtom(VtlParser.SubstrAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code substrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitSubstrAtom(VtlParser.SubstrAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code instrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterInstrAtom(VtlParser.InstrAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code instrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitInstrAtom(VtlParser.InstrAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replaceAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterReplaceAtom(VtlParser.ReplaceAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replaceAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitReplaceAtom(VtlParser.ReplaceAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code charsetMatchAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code charsetMatchAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code isNullAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterIsNullAtom(VtlParser.IsNullAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code isNullAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitIsNullAtom(VtlParser.IsNullAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nvlAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterNvlAtom(VtlParser.NvlAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nvlAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitNvlAtom(VtlParser.NvlAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterModAtom(VtlParser.ModAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitModAtom(VtlParser.ModAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterRefAtom(VtlParser.RefAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitRefAtom(VtlParser.RefAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code evalExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterEvalExprAtom(VtlParser.EvalExprAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code evalExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitEvalExprAtom(VtlParser.EvalExprAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code castExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterCastExprAtom(VtlParser.CastExprAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code castExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitCastExprAtom(VtlParser.CastExprAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code hierarchyExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterHierarchyExprAtom(VtlParser.HierarchyExprAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code hierarchyExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitHierarchyExprAtom(VtlParser.HierarchyExprAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code flowToStockAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterFlowToStockAtom(VtlParser.FlowToStockAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code flowToStockAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitFlowToStockAtom(VtlParser.FlowToStockAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stockToFlowAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterStockToFlowAtom(VtlParser.StockToFlowAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stockToFlowAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitStockToFlowAtom(VtlParser.StockToFlowAtomContext ctx);
	/**
	 * Enter a parse tree produced by the {@code validateDPruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code validateDPruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code validateHRruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code validateHRruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code validationSimple}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void enterValidationSimple(VtlParser.ValidationSimpleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code validationSimple}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 */
	void exitValidationSimple(VtlParser.ValidationSimpleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesisExprRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void enterParenthesisExprRef(VtlParser.ParenthesisExprRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesisExprRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void exitParenthesisExprRef(VtlParser.ParenthesisExprRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code varIdRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void enterVarIdRef(VtlParser.VarIdRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code varIdRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void exitVarIdRef(VtlParser.VarIdRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constantRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void enterConstantRef(VtlParser.ConstantRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constantRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 */
	void exitConstantRef(VtlParser.ConstantRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(VtlParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(VtlParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#lists}.
	 * @param ctx the parse tree
	 */
	void enterLists(VtlParser.ListsContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#lists}.
	 * @param ctx the parse tree
	 */
	void exitLists(VtlParser.ListsContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#evalExpr}.
	 * @param ctx the parse tree
	 */
	void enterEvalExpr(VtlParser.EvalExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#evalExpr}.
	 * @param ctx the parse tree
	 */
	void exitEvalExpr(VtlParser.EvalExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#castExpr}.
	 * @param ctx the parse tree
	 */
	void enterCastExpr(VtlParser.CastExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#castExpr}.
	 * @param ctx the parse tree
	 */
	void exitCastExpr(VtlParser.CastExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#periodExpr}.
	 * @param ctx the parse tree
	 */
	void enterPeriodExpr(VtlParser.PeriodExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#periodExpr}.
	 * @param ctx the parse tree
	 */
	void exitPeriodExpr(VtlParser.PeriodExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#timeShiftExpr}.
	 * @param ctx the parse tree
	 */
	void enterTimeShiftExpr(VtlParser.TimeShiftExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#timeShiftExpr}.
	 * @param ctx the parse tree
	 */
	void exitTimeShiftExpr(VtlParser.TimeShiftExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#timeSeriesExpr}.
	 * @param ctx the parse tree
	 */
	void enterTimeSeriesExpr(VtlParser.TimeSeriesExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#timeSeriesExpr}.
	 * @param ctx the parse tree
	 */
	void exitTimeSeriesExpr(VtlParser.TimeSeriesExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#timeAggExpr}.
	 * @param ctx the parse tree
	 */
	void enterTimeAggExpr(VtlParser.TimeAggExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#timeAggExpr}.
	 * @param ctx the parse tree
	 */
	void exitTimeAggExpr(VtlParser.TimeAggExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#validationExpr}.
	 * @param ctx the parse tree
	 */
	void enterValidationExpr(VtlParser.ValidationExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#validationExpr}.
	 * @param ctx the parse tree
	 */
	void exitValidationExpr(VtlParser.ValidationExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#validationDatapoint}.
	 * @param ctx the parse tree
	 */
	void enterValidationDatapoint(VtlParser.ValidationDatapointContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#validationDatapoint}.
	 * @param ctx the parse tree
	 */
	void exitValidationDatapoint(VtlParser.ValidationDatapointContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#validationHierarchical}.
	 * @param ctx the parse tree
	 */
	void enterValidationHierarchical(VtlParser.ValidationHierarchicalContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#validationHierarchical}.
	 * @param ctx the parse tree
	 */
	void exitValidationHierarchical(VtlParser.ValidationHierarchicalContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#erCode}.
	 * @param ctx the parse tree
	 */
	void enterErCode(VtlParser.ErCodeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#erCode}.
	 * @param ctx the parse tree
	 */
	void exitErCode(VtlParser.ErCodeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#erLevel}.
	 * @param ctx the parse tree
	 */
	void enterErLevel(VtlParser.ErLevelContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#erLevel}.
	 * @param ctx the parse tree
	 */
	void exitErLevel(VtlParser.ErLevelContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#hierarchyExpr}.
	 * @param ctx the parse tree
	 */
	void enterHierarchyExpr(VtlParser.HierarchyExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#hierarchyExpr}.
	 * @param ctx the parse tree
	 */
	void exitHierarchyExpr(VtlParser.HierarchyExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#datasetClause}.
	 * @param ctx the parse tree
	 */
	void enterDatasetClause(VtlParser.DatasetClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#datasetClause}.
	 * @param ctx the parse tree
	 */
	void exitDatasetClause(VtlParser.DatasetClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#anFunctionClause}.
	 * @param ctx the parse tree
	 */
	void enterAnFunctionClause(VtlParser.AnFunctionClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#anFunctionClause}.
	 * @param ctx the parse tree
	 */
	void exitAnFunctionClause(VtlParser.AnFunctionClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void enterPartitionByClause(VtlParser.PartitionByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void exitPartitionByClause(VtlParser.PartitionByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void enterOrderByClause(VtlParser.OrderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void exitOrderByClause(VtlParser.OrderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#windowingClause}.
	 * @param ctx the parse tree
	 */
	void enterWindowingClause(VtlParser.WindowingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#windowingClause}.
	 * @param ctx the parse tree
	 */
	void exitWindowingClause(VtlParser.WindowingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#limitClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterLimitClauseItem(VtlParser.LimitClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#limitClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitLimitClauseItem(VtlParser.LimitClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinExpr}.
	 * @param ctx the parse tree
	 */
	void enterJoinExpr(VtlParser.JoinExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinExpr}.
	 * @param ctx the parse tree
	 */
	void exitJoinExpr(VtlParser.JoinExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinClause(VtlParser.JoinClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinClause(VtlParser.JoinClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinBody}.
	 * @param ctx the parse tree
	 */
	void enterJoinBody(VtlParser.JoinBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinBody}.
	 * @param ctx the parse tree
	 */
	void exitJoinBody(VtlParser.JoinBodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinCalcClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinCalcClause(VtlParser.JoinCalcClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinCalcClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinCalcClause(VtlParser.JoinCalcClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinCalcClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterJoinCalcClauseItem(VtlParser.JoinCalcClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinCalcClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitJoinCalcClauseItem(VtlParser.JoinCalcClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinCalcExpr}.
	 * @param ctx the parse tree
	 */
	void enterJoinCalcExpr(VtlParser.JoinCalcExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinCalcExpr}.
	 * @param ctx the parse tree
	 */
	void exitJoinCalcExpr(VtlParser.JoinCalcExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinAggClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinAggClause(VtlParser.JoinAggClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinAggClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinAggClause(VtlParser.JoinAggClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinAggClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterJoinAggClauseItem(VtlParser.JoinAggClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinAggClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitJoinAggClauseItem(VtlParser.JoinAggClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinAggExpr}.
	 * @param ctx the parse tree
	 */
	void enterJoinAggExpr(VtlParser.JoinAggExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinAggExpr}.
	 * @param ctx the parse tree
	 */
	void exitJoinAggExpr(VtlParser.JoinAggExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinKeepClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinKeepClause(VtlParser.JoinKeepClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinKeepClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinKeepClause(VtlParser.JoinKeepClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinDropClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinDropClause(VtlParser.JoinDropClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinDropClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinDropClause(VtlParser.JoinDropClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinFilterClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinFilterClause(VtlParser.JoinFilterClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinFilterClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinFilterClause(VtlParser.JoinFilterClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinRenameClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinRenameClause(VtlParser.JoinRenameClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinRenameClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinRenameClause(VtlParser.JoinRenameClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinApplyClause}.
	 * @param ctx the parse tree
	 */
	void enterJoinApplyClause(VtlParser.JoinApplyClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinApplyClause}.
	 * @param ctx the parse tree
	 */
	void exitJoinApplyClause(VtlParser.JoinApplyClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#anFunction}.
	 * @param ctx the parse tree
	 */
	void enterAnFunction(VtlParser.AnFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#anFunction}.
	 * @param ctx the parse tree
	 */
	void exitAnFunction(VtlParser.AnFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggregateClause}.
	 * @param ctx the parse tree
	 */
	void enterAggregateClause(VtlParser.AggregateClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggregateClause}.
	 * @param ctx the parse tree
	 */
	void exitAggregateClause(VtlParser.AggregateClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrFunctionClause}.
	 * @param ctx the parse tree
	 */
	void enterAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrFunctionClause}.
	 * @param ctx the parse tree
	 */
	void exitAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#getFiltersClause}.
	 * @param ctx the parse tree
	 */
	void enterGetFiltersClause(VtlParser.GetFiltersClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#getFiltersClause}.
	 * @param ctx the parse tree
	 */
	void exitGetFiltersClause(VtlParser.GetFiltersClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#getFilterClause}.
	 * @param ctx the parse tree
	 */
	void enterGetFilterClause(VtlParser.GetFilterClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#getFilterClause}.
	 * @param ctx the parse tree
	 */
	void exitGetFilterClause(VtlParser.GetFilterClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrClause}.
	 * @param ctx the parse tree
	 */
	void enterAggrClause(VtlParser.AggrClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrClause}.
	 * @param ctx the parse tree
	 */
	void exitAggrClause(VtlParser.AggrClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#filterClause}.
	 * @param ctx the parse tree
	 */
	void enterFilterClause(VtlParser.FilterClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#filterClause}.
	 * @param ctx the parse tree
	 */
	void exitFilterClause(VtlParser.FilterClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#renameClause}.
	 * @param ctx the parse tree
	 */
	void enterRenameClause(VtlParser.RenameClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#renameClause}.
	 * @param ctx the parse tree
	 */
	void exitRenameClause(VtlParser.RenameClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrFunction}.
	 * @param ctx the parse tree
	 */
	void enterAggrFunction(VtlParser.AggrFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrFunction}.
	 * @param ctx the parse tree
	 */
	void exitAggrFunction(VtlParser.AggrFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#calcClause}.
	 * @param ctx the parse tree
	 */
	void enterCalcClause(VtlParser.CalcClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#calcClause}.
	 * @param ctx the parse tree
	 */
	void exitCalcClause(VtlParser.CalcClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#calcClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterCalcClauseItem(VtlParser.CalcClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#calcClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitCalcClauseItem(VtlParser.CalcClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#calcExpr}.
	 * @param ctx the parse tree
	 */
	void enterCalcExpr(VtlParser.CalcExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#calcExpr}.
	 * @param ctx the parse tree
	 */
	void exitCalcExpr(VtlParser.CalcExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#dropClause}.
	 * @param ctx the parse tree
	 */
	void enterDropClause(VtlParser.DropClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#dropClause}.
	 * @param ctx the parse tree
	 */
	void exitDropClause(VtlParser.DropClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#dropClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterDropClauseItem(VtlParser.DropClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#dropClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitDropClauseItem(VtlParser.DropClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#keepClause}.
	 * @param ctx the parse tree
	 */
	void enterKeepClause(VtlParser.KeepClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#keepClause}.
	 * @param ctx the parse tree
	 */
	void exitKeepClause(VtlParser.KeepClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#keepClauseItem}.
	 * @param ctx the parse tree
	 */
	void enterKeepClauseItem(VtlParser.KeepClauseItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#keepClauseItem}.
	 * @param ctx the parse tree
	 */
	void exitKeepClauseItem(VtlParser.KeepClauseItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#unpivotExpr}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotExpr(VtlParser.UnpivotExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#unpivotExpr}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotExpr(VtlParser.UnpivotExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#pivotExpr}.
	 * @param ctx the parse tree
	 */
	void enterPivotExpr(VtlParser.PivotExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#pivotExpr}.
	 * @param ctx the parse tree
	 */
	void exitPivotExpr(VtlParser.PivotExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#subspaceExpr}.
	 * @param ctx the parse tree
	 */
	void enterSubspaceExpr(VtlParser.SubspaceExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#subspaceExpr}.
	 * @param ctx the parse tree
	 */
	void exitSubspaceExpr(VtlParser.SubspaceExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#inBetweenClause}.
	 * @param ctx the parse tree
	 */
	void enterInBetweenClause(VtlParser.InBetweenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#inBetweenClause}.
	 * @param ctx the parse tree
	 */
	void exitInBetweenClause(VtlParser.InBetweenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#setExpr}.
	 * @param ctx the parse tree
	 */
	void enterSetExpr(VtlParser.SetExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#setExpr}.
	 * @param ctx the parse tree
	 */
	void exitSetExpr(VtlParser.SetExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#subscriptExpr}.
	 * @param ctx the parse tree
	 */
	void enterSubscriptExpr(VtlParser.SubscriptExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#subscriptExpr}.
	 * @param ctx the parse tree
	 */
	void exitSubscriptExpr(VtlParser.SubscriptExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrInvocation}.
	 * @param ctx the parse tree
	 */
	void enterAggrInvocation(VtlParser.AggrInvocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrInvocation}.
	 * @param ctx the parse tree
	 */
	void exitAggrInvocation(VtlParser.AggrInvocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrInvocationCompExpr}.
	 * @param ctx the parse tree
	 */
	void enterAggrInvocationCompExpr(VtlParser.AggrInvocationCompExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrInvocationCompExpr}.
	 * @param ctx the parse tree
	 */
	void exitAggrInvocationCompExpr(VtlParser.AggrInvocationCompExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#aggrFunctionName}.
	 * @param ctx the parse tree
	 */
	void enterAggrFunctionName(VtlParser.AggrFunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#aggrFunctionName}.
	 * @param ctx the parse tree
	 */
	void exitAggrFunctionName(VtlParser.AggrFunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#groupingClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupingClause(VtlParser.GroupingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#groupingClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupingClause(VtlParser.GroupingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(VtlParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(VtlParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#returnAll}.
	 * @param ctx the parse tree
	 */
	void enterReturnAll(VtlParser.ReturnAllContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#returnAll}.
	 * @param ctx the parse tree
	 */
	void exitReturnAll(VtlParser.ReturnAllContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#componentRole}.
	 * @param ctx the parse tree
	 */
	void enterComponentRole(VtlParser.ComponentRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#componentRole}.
	 * @param ctx the parse tree
	 */
	void exitComponentRole(VtlParser.ComponentRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#viralAttribute}.
	 * @param ctx the parse tree
	 */
	void enterViralAttribute(VtlParser.ViralAttributeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#viralAttribute}.
	 * @param ctx the parse tree
	 */
	void exitViralAttribute(VtlParser.ViralAttributeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#logBase}.
	 * @param ctx the parse tree
	 */
	void enterLogBase(VtlParser.LogBaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#logBase}.
	 * @param ctx the parse tree
	 */
	void exitLogBase(VtlParser.LogBaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#exponent}.
	 * @param ctx the parse tree
	 */
	void enterExponent(VtlParser.ExponentContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#exponent}.
	 * @param ctx the parse tree
	 */
	void exitExponent(VtlParser.ExponentContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#persistentDatasetID}.
	 * @param ctx the parse tree
	 */
	void enterPersistentDatasetID(VtlParser.PersistentDatasetIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#persistentDatasetID}.
	 * @param ctx the parse tree
	 */
	void exitPersistentDatasetID(VtlParser.PersistentDatasetIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#datasetID}.
	 * @param ctx the parse tree
	 */
	void enterDatasetID(VtlParser.DatasetIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#datasetID}.
	 * @param ctx the parse tree
	 */
	void exitDatasetID(VtlParser.DatasetIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#rulesetID}.
	 * @param ctx the parse tree
	 */
	void enterRulesetID(VtlParser.RulesetIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#rulesetID}.
	 * @param ctx the parse tree
	 */
	void exitRulesetID(VtlParser.RulesetIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#varID}.
	 * @param ctx the parse tree
	 */
	void enterVarID(VtlParser.VarIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#varID}.
	 * @param ctx the parse tree
	 */
	void exitVarID(VtlParser.VarIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#componentID}.
	 * @param ctx the parse tree
	 */
	void enterComponentID(VtlParser.ComponentIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#componentID}.
	 * @param ctx the parse tree
	 */
	void exitComponentID(VtlParser.ComponentIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#operatorID}.
	 * @param ctx the parse tree
	 */
	void enterOperatorID(VtlParser.OperatorIDContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#operatorID}.
	 * @param ctx the parse tree
	 */
	void exitOperatorID(VtlParser.OperatorIDContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#routineName}.
	 * @param ctx the parse tree
	 */
	void enterRoutineName(VtlParser.RoutineNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#routineName}.
	 * @param ctx the parse tree
	 */
	void exitRoutineName(VtlParser.RoutineNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#joinKeyword}.
	 * @param ctx the parse tree
	 */
	void enterJoinKeyword(VtlParser.JoinKeywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#joinKeyword}.
	 * @param ctx the parse tree
	 */
	void exitJoinKeyword(VtlParser.JoinKeywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#groupKeyword}.
	 * @param ctx the parse tree
	 */
	void enterGroupKeyword(VtlParser.GroupKeywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#groupKeyword}.
	 * @param ctx the parse tree
	 */
	void exitGroupKeyword(VtlParser.GroupKeywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(VtlParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(VtlParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#componentType2}.
	 * @param ctx the parse tree
	 */
	void enterComponentType2(VtlParser.ComponentType2Context ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#componentType2}.
	 * @param ctx the parse tree
	 */
	void exitComponentType2(VtlParser.ComponentType2Context ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#scalarType}.
	 * @param ctx the parse tree
	 */
	void enterScalarType(VtlParser.ScalarTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#scalarType}.
	 * @param ctx the parse tree
	 */
	void exitScalarType(VtlParser.ScalarTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#basicScalarType}.
	 * @param ctx the parse tree
	 */
	void enterBasicScalarType(VtlParser.BasicScalarTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#basicScalarType}.
	 * @param ctx the parse tree
	 */
	void exitBasicScalarType(VtlParser.BasicScalarTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#valueDomainName}.
	 * @param ctx the parse tree
	 */
	void enterValueDomainName(VtlParser.ValueDomainNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#valueDomainName}.
	 * @param ctx the parse tree
	 */
	void exitValueDomainName(VtlParser.ValueDomainNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#setName}.
	 * @param ctx the parse tree
	 */
	void enterSetName(VtlParser.SetNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#setName}.
	 * @param ctx the parse tree
	 */
	void exitSetName(VtlParser.SetNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#scalarTypeConstraint}.
	 * @param ctx the parse tree
	 */
	void enterScalarTypeConstraint(VtlParser.ScalarTypeConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#scalarTypeConstraint}.
	 * @param ctx the parse tree
	 */
	void exitScalarTypeConstraint(VtlParser.ScalarTypeConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterDataType(VtlParser.DataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitDataType(VtlParser.DataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#componentType}.
	 * @param ctx the parse tree
	 */
	void enterComponentType(VtlParser.ComponentTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#componentType}.
	 * @param ctx the parse tree
	 */
	void exitComponentType(VtlParser.ComponentTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#datasetType}.
	 * @param ctx the parse tree
	 */
	void enterDatasetType(VtlParser.DatasetTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#datasetType}.
	 * @param ctx the parse tree
	 */
	void exitDatasetType(VtlParser.DatasetTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#compConstraint}.
	 * @param ctx the parse tree
	 */
	void enterCompConstraint(VtlParser.CompConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#compConstraint}.
	 * @param ctx the parse tree
	 */
	void exitCompConstraint(VtlParser.CompConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#multModifier}.
	 * @param ctx the parse tree
	 */
	void enterMultModifier(VtlParser.MultModifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#multModifier}.
	 * @param ctx the parse tree
	 */
	void exitMultModifier(VtlParser.MultModifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#rulesetType}.
	 * @param ctx the parse tree
	 */
	void enterRulesetType(VtlParser.RulesetTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#rulesetType}.
	 * @param ctx the parse tree
	 */
	void exitRulesetType(VtlParser.RulesetTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#dpRuleset}.
	 * @param ctx the parse tree
	 */
	void enterDpRuleset(VtlParser.DpRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#dpRuleset}.
	 * @param ctx the parse tree
	 */
	void exitDpRuleset(VtlParser.DpRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#hrRuleset}.
	 * @param ctx the parse tree
	 */
	void enterHrRuleset(VtlParser.HrRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#hrRuleset}.
	 * @param ctx the parse tree
	 */
	void exitHrRuleset(VtlParser.HrRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#prodValueDomains}.
	 * @param ctx the parse tree
	 */
	void enterProdValueDomains(VtlParser.ProdValueDomainsContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#prodValueDomains}.
	 * @param ctx the parse tree
	 */
	void exitProdValueDomains(VtlParser.ProdValueDomainsContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#prodVariables}.
	 * @param ctx the parse tree
	 */
	void enterProdVariables(VtlParser.ProdVariablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#prodVariables}.
	 * @param ctx the parse tree
	 */
	void exitProdVariables(VtlParser.ProdVariablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#operatorType}.
	 * @param ctx the parse tree
	 */
	void enterOperatorType(VtlParser.OperatorTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#operatorType}.
	 * @param ctx the parse tree
	 */
	void exitOperatorType(VtlParser.OperatorTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#inputParameterType}.
	 * @param ctx the parse tree
	 */
	void enterInputParameterType(VtlParser.InputParameterTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#inputParameterType}.
	 * @param ctx the parse tree
	 */
	void exitInputParameterType(VtlParser.InputParameterTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#outputParameterType}.
	 * @param ctx the parse tree
	 */
	void enterOutputParameterType(VtlParser.OutputParameterTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#outputParameterType}.
	 * @param ctx the parse tree
	 */
	void exitOutputParameterType(VtlParser.OutputParameterTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#scalarSetType}.
	 * @param ctx the parse tree
	 */
	void enterScalarSetType(VtlParser.ScalarSetTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#scalarSetType}.
	 * @param ctx the parse tree
	 */
	void exitScalarSetType(VtlParser.ScalarSetTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#retainType}.
	 * @param ctx the parse tree
	 */
	void enterRetainType(VtlParser.RetainTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#retainType}.
	 * @param ctx the parse tree
	 */
	void exitRetainType(VtlParser.RetainTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defineDatapointRuleset}.
	 * @param ctx the parse tree
	 */
	void enterDefineDatapointRuleset(VtlParser.DefineDatapointRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defineDatapointRuleset}.
	 * @param ctx the parse tree
	 */
	void exitDefineDatapointRuleset(VtlParser.DefineDatapointRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defineHierarchicalRuleset}.
	 * @param ctx the parse tree
	 */
	void enterDefineHierarchicalRuleset(VtlParser.DefineHierarchicalRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defineHierarchicalRuleset}.
	 * @param ctx the parse tree
	 */
	void exitDefineHierarchicalRuleset(VtlParser.DefineHierarchicalRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#endDatapointRuleset}.
	 * @param ctx the parse tree
	 */
	void enterEndDatapointRuleset(VtlParser.EndDatapointRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#endDatapointRuleset}.
	 * @param ctx the parse tree
	 */
	void exitEndDatapointRuleset(VtlParser.EndDatapointRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#endHierarchicalRuleset}.
	 * @param ctx the parse tree
	 */
	void enterEndHierarchicalRuleset(VtlParser.EndHierarchicalRulesetContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#endHierarchicalRuleset}.
	 * @param ctx the parse tree
	 */
	void exitEndHierarchicalRuleset(VtlParser.EndHierarchicalRulesetContext ctx);
	/**
	 * Enter a parse tree produced by {@link VtlParser#defineDataStructure}.
	 * @param ctx the parse tree
	 */
	void enterDefineDataStructure(VtlParser.DefineDataStructureContext ctx);
	/**
	 * Exit a parse tree produced by {@link VtlParser#defineDataStructure}.
	 * @param ctx the parse tree
	 */
	void exitDefineDataStructure(VtlParser.DefineDataStructureContext ctx);
}