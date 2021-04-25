/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.util;

import org.eclipse.efbt.language.dtm.model.dtm_views.*;

import org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage
 * @generated
 */
public class dtm_viewsAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static dtm_viewsPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public dtm_viewsAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = dtm_viewsPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected dtm_viewsSwitch<Adapter> modelSwitch =
		new dtm_viewsSwitch<Adapter>() {
			@Override
			public Adapter caseAggregateEnrichmentView(AggregateEnrichmentView object) {
				return createAggregateEnrichmentViewAdapter();
			}
			@Override
			public Adapter caseCopyView(CopyView object) {
				return createCopyViewAdapter();
			}
			@Override
			public Adapter caseEnrichmentView(EnrichmentView object) {
				return createEnrichmentViewAdapter();
			}
			@Override
			public Adapter caseFilterByConditionView(FilterByConditionView object) {
				return createFilterByConditionViewAdapter();
			}
			@Override
			public Adapter caseGenericView(GenericView object) {
				return createGenericViewAdapter();
			}
			@Override
			public Adapter caseGroupByClause(GroupByClause object) {
				return createGroupByClauseAdapter();
			}
			@Override
			public Adapter caseIgnoreColumn(IgnoreColumn object) {
				return createIgnoreColumnAdapter();
			}
			@Override
			public Adapter caseJoinView(JoinView object) {
				return createJoinViewAdapter();
			}
			@Override
			public Adapter caseRenameAs(RenameAs object) {
				return createRenameAsAdapter();
			}
			@Override
			public Adapter caseSelectClause(SelectClause object) {
				return createSelectClauseAdapter();
			}
			@Override
			public Adapter caseDTMView(DTMView object) {
				return createDTMViewAdapter();
			}
			@Override
			public Adapter caseUnionView(UnionView object) {
				return createUnionViewAdapter();
			}
			@Override
			public Adapter caseVersionedDTMViewsModule(VersionedDTMViewsModule object) {
				return createVersionedDTMViewsModuleAdapter();
			}
			@Override
			public Adapter caseWhereClause(WhereClause object) {
				return createWhereClauseAdapter();
			}
			@Override
			public Adapter caseCastColumnView(CastColumnView object) {
				return createCastColumnViewAdapter();
			}
			@Override
			public Adapter caseExplodeArrayOfStructsView(ExplodeArrayOfStructsView object) {
				return createExplodeArrayOfStructsViewAdapter();
			}
			@Override
			public Adapter caseExplodeStructView(ExplodeStructView object) {
				return createExplodeStructViewAdapter();
			}
			@Override
			public Adapter caseFilterByStructClassColumnView(FilterByStructClassColumnView object) {
				return createFilterByStructClassColumnViewAdapter();
			}
			@Override
			public Adapter caseMakeArrayOfStructsView(MakeArrayOfStructsView object) {
				return createMakeArrayOfStructsViewAdapter();
			}
			@Override
			public Adapter caseMakeStructView(MakeStructView object) {
				return createMakeStructViewAdapter();
			}
			@Override
			public Adapter caseAggregateEnrichmentViewAndOrderBy(AggregateEnrichmentViewAndOrderBy object) {
				return createAggregateEnrichmentViewAndOrderByAdapter();
			}
			@Override
			public Adapter caseDataSetTransformation(DataSetTransformation object) {
				return createDataSetTransformationAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object) {
				return createVersionedFunctionalModuleLogicAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView <em>Aggregate Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView
	 * @generated
	 */
	public Adapter createAggregateEnrichmentViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CopyView <em>Copy View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CopyView
	 * @generated
	 */
	public Adapter createCopyViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView <em>Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView
	 * @generated
	 */
	public Adapter createEnrichmentViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView <em>Filter By Condition View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView
	 * @generated
	 */
	public Adapter createFilterByConditionViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GenericView <em>Generic View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GenericView
	 * @generated
	 */
	public Adapter createGenericViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause
	 * @generated
	 */
	public Adapter createGroupByClauseAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn <em>Ignore Column</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn
	 * @generated
	 */
	public Adapter createIgnoreColumnAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.JoinView <em>Join View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.JoinView
	 * @generated
	 */
	public Adapter createJoinViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs <em>Rename As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs
	 * @generated
	 */
	public Adapter createRenameAsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause
	 * @generated
	 */
	public Adapter createSelectClauseAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.DTMView <em>DTM View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.DTMView
	 * @generated
	 */
	public Adapter createDTMViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.UnionView <em>Union View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.UnionView
	 * @generated
	 */
	public Adapter createUnionViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule <em>Versioned DTM Views Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule
	 * @generated
	 */
	public Adapter createVersionedDTMViewsModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause
	 * @generated
	 */
	public Adapter createWhereClauseAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView <em>Cast Column View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView
	 * @generated
	 */
	public Adapter createCastColumnViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView <em>Explode Array Of Structs View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView
	 * @generated
	 */
	public Adapter createExplodeArrayOfStructsViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView <em>Explode Struct View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView
	 * @generated
	 */
	public Adapter createExplodeStructViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView <em>Filter By Struct Class Column View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView
	 * @generated
	 */
	public Adapter createFilterByStructClassColumnViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView <em>Make Array Of Structs View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView
	 * @generated
	 */
	public Adapter createMakeArrayOfStructsViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView <em>Make Struct View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView
	 * @generated
	 */
	public Adapter createMakeStructViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy <em>Aggregate Enrichment View And Order By</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy
	 * @generated
	 */
	public Adapter createAggregateEnrichmentViewAndOrderByAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation <em>Data Set Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation
	 * @generated
	 */
	public Adapter createDataSetTransformationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic <em>Versioned Functional Module Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic
	 * @generated
	 */
	public Adapter createVersionedFunctionalModuleLogicAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //dtm_viewsAdapterFactory
