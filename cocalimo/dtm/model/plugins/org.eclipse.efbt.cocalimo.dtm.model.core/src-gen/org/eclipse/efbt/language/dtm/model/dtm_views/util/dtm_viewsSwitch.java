/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.util;

import org.eclipse.efbt.language.dtm.model.dtm_views.*;

import org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic;

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
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage
 * @generated
 */
public class dtm_viewsSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static dtm_viewsPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public dtm_viewsSwitch() {
		if (modelPackage == null) {
			modelPackage = dtm_viewsPackage.eINSTANCE;
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
	protected boolean isSwitchFor(EPackage ePackage) {
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
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW: {
				AggregateEnrichmentView aggregateEnrichmentView = (AggregateEnrichmentView)theEObject;
				T result = caseAggregateEnrichmentView(aggregateEnrichmentView);
				if (result == null) result = caseDTMView(aggregateEnrichmentView);
				if (result == null) result = caseDataSetTransformation(aggregateEnrichmentView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.COPY_VIEW: {
				CopyView copyView = (CopyView)theEObject;
				T result = caseCopyView(copyView);
				if (result == null) result = caseDTMView(copyView);
				if (result == null) result = caseDataSetTransformation(copyView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.ENRICHMENT_VIEW: {
				EnrichmentView enrichmentView = (EnrichmentView)theEObject;
				T result = caseEnrichmentView(enrichmentView);
				if (result == null) result = caseDTMView(enrichmentView);
				if (result == null) result = caseDataSetTransformation(enrichmentView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.FILTER_BY_CONDITION_VIEW: {
				FilterByConditionView filterByConditionView = (FilterByConditionView)theEObject;
				T result = caseFilterByConditionView(filterByConditionView);
				if (result == null) result = caseDTMView(filterByConditionView);
				if (result == null) result = caseDataSetTransformation(filterByConditionView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.GENERIC_VIEW: {
				GenericView genericView = (GenericView)theEObject;
				T result = caseGenericView(genericView);
				if (result == null) result = caseDTMView(genericView);
				if (result == null) result = caseDataSetTransformation(genericView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.GROUP_BY_CLAUSE: {
				GroupByClause groupByClause = (GroupByClause)theEObject;
				T result = caseGroupByClause(groupByClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.IGNORE_COLUMN: {
				IgnoreColumn ignoreColumn = (IgnoreColumn)theEObject;
				T result = caseIgnoreColumn(ignoreColumn);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.JOIN_VIEW: {
				JoinView joinView = (JoinView)theEObject;
				T result = caseJoinView(joinView);
				if (result == null) result = caseDTMView(joinView);
				if (result == null) result = caseDataSetTransformation(joinView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.RENAME_AS: {
				RenameAs renameAs = (RenameAs)theEObject;
				T result = caseRenameAs(renameAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.SELECT_CLAUSE: {
				SelectClause selectClause = (SelectClause)theEObject;
				T result = caseSelectClause(selectClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.DTM_VIEW: {
				DTMView dtmView = (DTMView)theEObject;
				T result = caseDTMView(dtmView);
				if (result == null) result = caseDataSetTransformation(dtmView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.UNION_VIEW: {
				UnionView unionView = (UnionView)theEObject;
				T result = caseUnionView(unionView);
				if (result == null) result = caseDTMView(unionView);
				if (result == null) result = caseDataSetTransformation(unionView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE: {
				VersionedDTMViewsModule versionedDTMViewsModule = (VersionedDTMViewsModule)theEObject;
				T result = caseVersionedDTMViewsModule(versionedDTMViewsModule);
				if (result == null) result = caseVersionedFunctionalModuleLogic(versionedDTMViewsModule);
				if (result == null) result = caseModule(versionedDTMViewsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.WHERE_CLAUSE: {
				WhereClause whereClause = (WhereClause)theEObject;
				T result = caseWhereClause(whereClause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.CAST_COLUMN_VIEW: {
				CastColumnView castColumnView = (CastColumnView)theEObject;
				T result = caseCastColumnView(castColumnView);
				if (result == null) result = caseDTMView(castColumnView);
				if (result == null) result = caseDataSetTransformation(castColumnView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW: {
				ExplodeArrayOfStructsView explodeArrayOfStructsView = (ExplodeArrayOfStructsView)theEObject;
				T result = caseExplodeArrayOfStructsView(explodeArrayOfStructsView);
				if (result == null) result = caseDTMView(explodeArrayOfStructsView);
				if (result == null) result = caseDataSetTransformation(explodeArrayOfStructsView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.EXPLODE_STRUCT_VIEW: {
				ExplodeStructView explodeStructView = (ExplodeStructView)theEObject;
				T result = caseExplodeStructView(explodeStructView);
				if (result == null) result = caseDTMView(explodeStructView);
				if (result == null) result = caseDataSetTransformation(explodeStructView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW: {
				FilterByStructClassColumnView filterByStructClassColumnView = (FilterByStructClassColumnView)theEObject;
				T result = caseFilterByStructClassColumnView(filterByStructClassColumnView);
				if (result == null) result = caseDTMView(filterByStructClassColumnView);
				if (result == null) result = caseDataSetTransformation(filterByStructClassColumnView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW: {
				MakeArrayOfStructsView makeArrayOfStructsView = (MakeArrayOfStructsView)theEObject;
				T result = caseMakeArrayOfStructsView(makeArrayOfStructsView);
				if (result == null) result = caseDTMView(makeArrayOfStructsView);
				if (result == null) result = caseDataSetTransformation(makeArrayOfStructsView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.MAKE_STRUCT_VIEW: {
				MakeStructView makeStructView = (MakeStructView)theEObject;
				T result = caseMakeStructView(makeStructView);
				if (result == null) result = caseDTMView(makeStructView);
				if (result == null) result = caseDataSetTransformation(makeStructView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY: {
				AggregateEnrichmentViewAndOrderBy aggregateEnrichmentViewAndOrderBy = (AggregateEnrichmentViewAndOrderBy)theEObject;
				T result = caseAggregateEnrichmentViewAndOrderBy(aggregateEnrichmentViewAndOrderBy);
				if (result == null) result = caseDTMView(aggregateEnrichmentViewAndOrderBy);
				if (result == null) result = caseDataSetTransformation(aggregateEnrichmentViewAndOrderBy);
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
	public T caseAggregateEnrichmentView(AggregateEnrichmentView object) {
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
	public T caseCopyView(CopyView object) {
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
	public T caseEnrichmentView(EnrichmentView object) {
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
	public T caseFilterByConditionView(FilterByConditionView object) {
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
	public T caseGenericView(GenericView object) {
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
	public T caseGroupByClause(GroupByClause object) {
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
	public T caseIgnoreColumn(IgnoreColumn object) {
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
	public T caseJoinView(JoinView object) {
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
	public T caseRenameAs(RenameAs object) {
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
	public T caseSelectClause(SelectClause object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>DTM View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>DTM View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDTMView(DTMView object) {
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
	public T caseUnionView(UnionView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned DTM Views Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned DTM Views Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedDTMViewsModule(VersionedDTMViewsModule object) {
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
	public T caseWhereClause(WhereClause object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cast Column View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cast Column View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCastColumnView(CastColumnView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Explode Array Of Structs View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Explode Array Of Structs View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseExplodeArrayOfStructsView(ExplodeArrayOfStructsView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Explode Struct View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Explode Struct View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseExplodeStructView(ExplodeStructView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter By Struct Class Column View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter By Struct Class Column View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterByStructClassColumnView(FilterByStructClassColumnView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Make Array Of Structs View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Make Array Of Structs View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMakeArrayOfStructsView(MakeArrayOfStructsView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Make Struct View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Make Struct View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMakeStructView(MakeStructView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Enrichment View And Order By</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Enrichment View And Order By</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateEnrichmentViewAndOrderBy(AggregateEnrichmentViewAndOrderBy object) {
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
	public T caseDataSetTransformation(DataSetTransformation object) {
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
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
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
	public T caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object) {
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
	public T defaultCase(EObject object) {
		return null;
	}

} //dtm_viewsSwitch
