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
package org.eclipse.efbt.component.attributelineagecreator.parser.vtl;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link VtlParser}.
 * @author Neil Mackenzie
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface VtlVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link VtlParser#start}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStart(VtlParser.StartContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(VtlParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#persistentAssignment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPersistentAssignment(VtlParser.PersistentAssignmentContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#optionalExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOptionalExpr(VtlParser.OptionalExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpr(VtlParser.ExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exprValidationExpr}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExprValidationExpr(VtlParser.ExprValidationExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code definitionExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefinitionExpressions(VtlParser.DefinitionExpressionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code standaloneAggregateFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStandaloneAggregateFunction(VtlParser.StandaloneAggregateFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code componentExpressionwithAggrClause}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComponentExpressionwithAggrClause(VtlParser.ComponentExpressionwithAggrClauseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code standaloneAnalyticFunction}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStandaloneAnalyticFunction(VtlParser.StandaloneAnalyticFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleaggregateFunctions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleaggregateFunctions(VtlParser.SimpleaggregateFunctionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timeexpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeexpressions(VtlParser.TimeexpressionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setExpressions}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetExpressions(VtlParser.SetExpressionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code callFunctionExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCallFunctionExpression(VtlParser.CallFunctionExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code joinExpression}
	 * labeled alternative in {@link VtlParser#exprComplex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinExpression(VtlParser.JoinExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#timeExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeExpr(VtlParser.TimeExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defHierarchical}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefHierarchical(VtlParser.DefHierarchicalContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#ruleClauseHierarchical}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleClauseHierarchical(VtlParser.RuleClauseHierarchicalContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#ruleItemHierarchical}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleItemHierarchical(VtlParser.RuleItemHierarchicalContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#hierRuleSignature}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHierRuleSignature(VtlParser.HierRuleSignatureContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#valueDomainSignature}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueDomainSignature(VtlParser.ValueDomainSignatureContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#codeItemRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCodeItemRelation(VtlParser.CodeItemRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#codeItemRelationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCodeItemRelationClause(VtlParser.CodeItemRelationClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#codeItemRef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCodeItemRef(VtlParser.CodeItemRefContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defDatapoint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefDatapoint(VtlParser.DefDatapointContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#ruleClauseDatapoint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleClauseDatapoint(VtlParser.RuleClauseDatapointContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#ruleItemDatapoint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleItemDatapoint(VtlParser.RuleItemDatapointContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#rulesetSignature}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRulesetSignature(VtlParser.RulesetSignatureContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#varSignature}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVarSignature(VtlParser.VarSignatureContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefExpr(VtlParser.DefExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefOperator(VtlParser.DefOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#parameterItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParameterItem(VtlParser.ParameterItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#callFunction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCallFunction(VtlParser.CallFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code roundAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRoundAtom(VtlParser.RoundAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ceilAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCeilAtom(VtlParser.CeilAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code floorAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloorAtom(VtlParser.FloorAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code minAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMinAtom(VtlParser.MinAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code expAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpAtom(VtlParser.ExpAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lnAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLnAtom(VtlParser.LnAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogAtom(VtlParser.LogAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code powerAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPowerAtom(VtlParser.PowerAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sqrtAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSqrtAtom(VtlParser.SqrtAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLenAtom(VtlParser.LenAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code betweenAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBetweenAtom(VtlParser.BetweenAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code trimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTrimAtom(VtlParser.TrimAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ltrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLtrimAtom(VtlParser.LtrimAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rtrimAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRtrimAtom(VtlParser.RtrimAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ucaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUcaseAtom(VtlParser.UcaseAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lcaseAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLcaseAtom(VtlParser.LcaseAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code substrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstrAtom(VtlParser.SubstrAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code instrAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInstrAtom(VtlParser.InstrAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code replaceAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReplaceAtom(VtlParser.ReplaceAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code charsetMatchAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code isNullAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsNullAtom(VtlParser.IsNullAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nvlAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNvlAtom(VtlParser.NvlAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code modAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitModAtom(VtlParser.ModAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefAtom(VtlParser.RefAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code evalExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEvalExprAtom(VtlParser.EvalExprAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code castExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCastExprAtom(VtlParser.CastExprAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code hierarchyExprAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHierarchyExprAtom(VtlParser.HierarchyExprAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code flowToStockAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFlowToStockAtom(VtlParser.FlowToStockAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stockToFlowAtom}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStockToFlowAtom(VtlParser.StockToFlowAtomContext ctx);
	/**
	 * Visit a parse tree produced by the {@code validateDPruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx);
	/**
	 * Visit a parse tree produced by the {@code validateHRruleset}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx);
	/**
	 * Visit a parse tree produced by the {@code validationSimple}
	 * labeled alternative in {@link VtlParser#exprAtom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidationSimple(VtlParser.ValidationSimpleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesisExprRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesisExprRef(VtlParser.ParenthesisExprRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code varIdRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVarIdRef(VtlParser.VarIdRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code constantRef}
	 * labeled alternative in {@link VtlParser#ref}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantRef(VtlParser.ConstantRefContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#identifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierList(VtlParser.IdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#lists}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLists(VtlParser.ListsContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#evalExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEvalExpr(VtlParser.EvalExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#castExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCastExpr(VtlParser.CastExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#periodExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPeriodExpr(VtlParser.PeriodExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#timeShiftExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeShiftExpr(VtlParser.TimeShiftExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#timeSeriesExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeSeriesExpr(VtlParser.TimeSeriesExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#timeAggExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeAggExpr(VtlParser.TimeAggExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#validationExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidationExpr(VtlParser.ValidationExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#validationDatapoint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidationDatapoint(VtlParser.ValidationDatapointContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#validationHierarchical}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValidationHierarchical(VtlParser.ValidationHierarchicalContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#erCode}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErCode(VtlParser.ErCodeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#erLevel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErLevel(VtlParser.ErLevelContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#hierarchyExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHierarchyExpr(VtlParser.HierarchyExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#datasetClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatasetClause(VtlParser.DatasetClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#anFunctionClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnFunctionClause(VtlParser.AnFunctionClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#partitionByClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionByClause(VtlParser.PartitionByClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#orderByClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderByClause(VtlParser.OrderByClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#windowingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowingClause(VtlParser.WindowingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#limitClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLimitClauseItem(VtlParser.LimitClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinExpr(VtlParser.JoinExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinClause(VtlParser.JoinClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinBody(VtlParser.JoinBodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinCalcClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCalcClause(VtlParser.JoinCalcClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinCalcClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCalcClauseItem(VtlParser.JoinCalcClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinCalcExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCalcExpr(VtlParser.JoinCalcExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinAggClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinAggClause(VtlParser.JoinAggClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinAggClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinAggClauseItem(VtlParser.JoinAggClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinAggExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinAggExpr(VtlParser.JoinAggExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinKeepClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinKeepClause(VtlParser.JoinKeepClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinDropClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinDropClause(VtlParser.JoinDropClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinFilterClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinFilterClause(VtlParser.JoinFilterClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinRenameClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinRenameClause(VtlParser.JoinRenameClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinApplyClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinApplyClause(VtlParser.JoinApplyClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#anFunction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnFunction(VtlParser.AnFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggregateClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregateClause(VtlParser.AggregateClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrFunctionClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#getFiltersClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGetFiltersClause(VtlParser.GetFiltersClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#getFilterClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGetFilterClause(VtlParser.GetFilterClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrClause(VtlParser.AggrClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#filterClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFilterClause(VtlParser.FilterClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#renameClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameClause(VtlParser.RenameClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrFunction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrFunction(VtlParser.AggrFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#calcClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCalcClause(VtlParser.CalcClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#calcClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCalcClauseItem(VtlParser.CalcClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#calcExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCalcExpr(VtlParser.CalcExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#dropClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropClause(VtlParser.DropClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#dropClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropClauseItem(VtlParser.DropClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#keepClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeepClause(VtlParser.KeepClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#keepClauseItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeepClauseItem(VtlParser.KeepClauseItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#unpivotExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotExpr(VtlParser.UnpivotExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#pivotExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPivotExpr(VtlParser.PivotExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#subspaceExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubspaceExpr(VtlParser.SubspaceExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#inBetweenClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInBetweenClause(VtlParser.InBetweenClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#setExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetExpr(VtlParser.SetExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#subscriptExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscriptExpr(VtlParser.SubscriptExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrInvocation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrInvocation(VtlParser.AggrInvocationContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrInvocationCompExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrInvocationCompExpr(VtlParser.AggrInvocationCompExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#aggrFunctionName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggrFunctionName(VtlParser.AggrFunctionNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#groupingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingClause(VtlParser.GroupingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#havingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHavingClause(VtlParser.HavingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#returnAll}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReturnAll(VtlParser.ReturnAllContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#componentRole}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComponentRole(VtlParser.ComponentRoleContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#viralAttribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitViralAttribute(VtlParser.ViralAttributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#logBase}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogBase(VtlParser.LogBaseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#exponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExponent(VtlParser.ExponentContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#persistentDatasetID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPersistentDatasetID(VtlParser.PersistentDatasetIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#datasetID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatasetID(VtlParser.DatasetIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#rulesetID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRulesetID(VtlParser.RulesetIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#varID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVarID(VtlParser.VarIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#componentID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComponentID(VtlParser.ComponentIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#operatorID}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperatorID(VtlParser.OperatorIDContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#routineName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRoutineName(VtlParser.RoutineNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#joinKeyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinKeyword(VtlParser.JoinKeywordContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#groupKeyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupKeyword(VtlParser.GroupKeywordContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstant(VtlParser.ConstantContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#componentType2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComponentType2(VtlParser.ComponentType2Context ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#scalarType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScalarType(VtlParser.ScalarTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#basicScalarType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBasicScalarType(VtlParser.BasicScalarTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#valueDomainName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueDomainName(VtlParser.ValueDomainNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#setName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetName(VtlParser.SetNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#scalarTypeConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScalarTypeConstraint(VtlParser.ScalarTypeConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDataType(VtlParser.DataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#componentType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComponentType(VtlParser.ComponentTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#datasetType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatasetType(VtlParser.DatasetTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#compConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompConstraint(VtlParser.CompConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#multModifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultModifier(VtlParser.MultModifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#rulesetType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRulesetType(VtlParser.RulesetTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#dpRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDpRuleset(VtlParser.DpRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#hrRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHrRuleset(VtlParser.HrRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#prodValueDomains}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProdValueDomains(VtlParser.ProdValueDomainsContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#prodVariables}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProdVariables(VtlParser.ProdVariablesContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#operatorType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperatorType(VtlParser.OperatorTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#inputParameterType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInputParameterType(VtlParser.InputParameterTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#outputParameterType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOutputParameterType(VtlParser.OutputParameterTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#scalarSetType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScalarSetType(VtlParser.ScalarSetTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#retainType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRetainType(VtlParser.RetainTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defineDatapointRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefineDatapointRuleset(VtlParser.DefineDatapointRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defineHierarchicalRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefineHierarchicalRuleset(VtlParser.DefineHierarchicalRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#endDatapointRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEndDatapointRuleset(VtlParser.EndDatapointRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#endHierarchicalRuleset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEndHierarchicalRuleset(VtlParser.EndHierarchicalRulesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link VtlParser#defineDataStructure}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefineDataStructure(VtlParser.DefineDataStructureContext ctx);
}