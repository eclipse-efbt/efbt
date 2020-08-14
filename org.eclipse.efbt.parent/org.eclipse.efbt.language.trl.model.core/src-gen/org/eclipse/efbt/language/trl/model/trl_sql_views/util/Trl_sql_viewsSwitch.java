/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.util;

import org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.efbt.language.trl.model.trl_sql_views.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage
 * @generated
 */
public class Trl_sql_viewsSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Trl_sql_viewsPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Trl_sql_viewsSwitch()
	{
		if (modelPackage == null)
		{
			modelPackage = Trl_sql_viewsPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage)
	{
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject)
	{
		switch (classifierID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW:
			{
				AggregateEnrichmentView aggregateEnrichmentView = (AggregateEnrichmentView)theEObject;
				T result = caseAggregateEnrichmentView(aggregateEnrichmentView);
				if (result == null) result = caseSQLView(aggregateEnrichmentView);
				if (result == null) result = caseDataSetTransformation(aggregateEnrichmentView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.COPY_VIEW:
			{
				CopyView copyView = (CopyView)theEObject;
				T result = caseCopyView(copyView);
				if (result == null) result = caseSQLView(copyView);
				if (result == null) result = caseDataSetTransformation(copyView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW:
			{
				EnrichmentView enrichmentView = (EnrichmentView)theEObject;
				T result = caseEnrichmentView(enrichmentView);
				if (result == null) result = caseSQLView(enrichmentView);
				if (result == null) result = caseDataSetTransformation(enrichmentView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.FILTER_BY_CONDITION_VIEW:
			{
				FilterByConditionView filterByConditionView = (FilterByConditionView)theEObject;
				T result = caseFilterByConditionView(filterByConditionView);
				if (result == null) result = caseSQLView(filterByConditionView);
				if (result == null) result = caseDataSetTransformation(filterByConditionView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.GENERIC_VIEW:
			{
				GenericView genericView = (GenericView)theEObject;
				T result = caseGenericView(genericView);
				if (result == null) result = caseSQLView(genericView);
				if (result == null) result = caseDataSetTransformation(genericView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.GROUP_BY_CLAUSE:
			{
				GroupByClause groupByClause = (GroupByClause)theEObject;
				T result = caseGroupByClause(groupByClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.IGNORE_COLUMN:
			{
				IgnoreColumn ignoreColumn = (IgnoreColumn)theEObject;
				T result = caseIgnoreColumn(ignoreColumn);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.JOIN_VIEW:
			{
				JoinView joinView = (JoinView)theEObject;
				T result = caseJoinView(joinView);
				if (result == null) result = caseSQLView(joinView);
				if (result == null) result = caseDataSetTransformation(joinView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.RENAME_AS:
			{
				RenameAs renameAs = (RenameAs)theEObject;
				T result = caseRenameAs(renameAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.SELECT_CLAUSE:
			{
				SelectClause selectClause = (SelectClause)theEObject;
				T result = caseSelectClause(selectClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.SQL_VIEW:
			{
				SQLView sqlView = (SQLView)theEObject;
				T result = caseSQLView(sqlView);
				if (result == null) result = caseDataSetTransformation(sqlView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.UNION_VIEW:
			{
				UnionView unionView = (UnionView)theEObject;
				T result = caseUnionView(unionView);
				if (result == null) result = caseSQLView(unionView);
				if (result == null) result = caseDataSetTransformation(unionView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE:
			{
				VersionedSQLViewsModule versionedSQLViewsModule = (VersionedSQLViewsModule)theEObject;
				T result = caseVersionedSQLViewsModule(versionedSQLViewsModule);
				if (result == null) result = caseVersionedFunctionalModuleLogic(versionedSQLViewsModule);
				if (result == null) result = caseModule(versionedSQLViewsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.WHERE_CLAUSE:
			{
				WhereClause whereClause = (WhereClause)theEObject;
				T result = caseWhereClause(whereClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS:
			{
				BaseViewIncorporatingDeltas baseViewIncorporatingDeltas = (BaseViewIncorporatingDeltas)theEObject;
				T result = caseBaseViewIncorporatingDeltas(baseViewIncorporatingDeltas);
				if (result == null) result = caseSQLView(baseViewIncorporatingDeltas);
				if (result == null) result = caseDataSetTransformation(baseViewIncorporatingDeltas);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Trl_sql_viewsPackage.DELTA_ACCUMULATION:
			{
				DeltaAccumulation deltaAccumulation = (DeltaAccumulation)theEObject;
				T result = caseDeltaAccumulation(deltaAccumulation);
				if (result == null) result = caseSQLView(deltaAccumulation);
				if (result == null) result = caseDataSetTransformation(deltaAccumulation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Enrichment View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Enrichment View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateEnrichmentView(AggregateEnrichmentView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Copy View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Copy View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCopyView(CopyView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Enrichment View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Enrichment View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEnrichmentView(EnrichmentView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter By Condition View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter By Condition View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterByConditionView(FilterByConditionView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generic View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generic View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGenericView(GenericView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Group By Clause</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Group By Clause</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGroupByClause(GroupByClause object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Ignore Column</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Ignore Column</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseIgnoreColumn(IgnoreColumn object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Join View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Join View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseJoinView(JoinView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rename As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rename As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRenameAs(RenameAs object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Clause</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Clause</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectClause(SelectClause object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SQL View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SQL View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSQLView(SQLView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Union View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Union View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUnionView(UnionView object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned SQL Views Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned SQL Views Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedSQLViewsModule(VersionedSQLViewsModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Where Clause</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Where Clause</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseWhereClause(WhereClause object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base View Incorporating Deltas</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base View Incorporating Deltas</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseViewIncorporatingDeltas(BaseViewIncorporatingDeltas object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Delta Accumulation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Delta Accumulation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDeltaAccumulation(DeltaAccumulation object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Data Set Transformation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Data Set Transformation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDataSetTransformation(DataSetTransformation object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.common.model.module_management.Module object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned Functional Module Logic</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned Functional Module Logic</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object)
	{
		return null;
	}

} //Trl_sql_viewsSwitch
