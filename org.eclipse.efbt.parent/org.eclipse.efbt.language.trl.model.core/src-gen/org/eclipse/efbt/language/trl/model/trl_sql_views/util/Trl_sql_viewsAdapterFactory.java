/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.util;

import org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.efbt.language.trl.model.trl_sql_views.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage
 * @generated
 */
public class Trl_sql_viewsAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Trl_sql_viewsPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Trl_sql_viewsAdapterFactory()
	{
		if (modelPackage == null)
		{
			modelPackage = Trl_sql_viewsPackage.eINSTANCE;
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
	public boolean isFactoryForType(Object object)
	{
		if (object == modelPackage)
		{
			return true;
		}
		if (object instanceof EObject)
		{
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
	protected Trl_sql_viewsSwitch<Adapter> modelSwitch =
		new Trl_sql_viewsSwitch<Adapter>()
		{
			@Override
			public Adapter caseAggregateEnrichmentView(AggregateEnrichmentView object)
			{
				return createAggregateEnrichmentViewAdapter();
			}
			@Override
			public Adapter caseCopyView(CopyView object)
			{
				return createCopyViewAdapter();
			}
			@Override
			public Adapter caseEnrichmentView(EnrichmentView object)
			{
				return createEnrichmentViewAdapter();
			}
			@Override
			public Adapter caseFilterByConditionView(FilterByConditionView object)
			{
				return createFilterByConditionViewAdapter();
			}
			@Override
			public Adapter caseGenericView(GenericView object)
			{
				return createGenericViewAdapter();
			}
			@Override
			public Adapter caseGroupByClause(GroupByClause object)
			{
				return createGroupByClauseAdapter();
			}
			@Override
			public Adapter caseIgnoreColumn(IgnoreColumn object)
			{
				return createIgnoreColumnAdapter();
			}
			@Override
			public Adapter caseJoinView(JoinView object)
			{
				return createJoinViewAdapter();
			}
			@Override
			public Adapter caseRenameAs(RenameAs object)
			{
				return createRenameAsAdapter();
			}
			@Override
			public Adapter caseSelectClause(SelectClause object)
			{
				return createSelectClauseAdapter();
			}
			@Override
			public Adapter caseSQLView(SQLView object)
			{
				return createSQLViewAdapter();
			}
			@Override
			public Adapter caseUnionView(UnionView object)
			{
				return createUnionViewAdapter();
			}
			@Override
			public Adapter caseVersionedSQLViewsModule(VersionedSQLViewsModule object)
			{
				return createVersionedSQLViewsModuleAdapter();
			}
			@Override
			public Adapter caseWhereClause(WhereClause object)
			{
				return createWhereClauseAdapter();
			}
			@Override
			public Adapter caseBaseViewIncorporatingDeltas(BaseViewIncorporatingDeltas object)
			{
				return createBaseViewIncorporatingDeltasAdapter();
			}
			@Override
			public Adapter caseDeltaAccumulation(DeltaAccumulation object)
			{
				return createDeltaAccumulationAdapter();
			}
			@Override
			public Adapter caseDataSetTransformation(DataSetTransformation object)
			{
				return createDataSetTransformationAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.common.model.module_management.Module object)
			{
				return createModuleAdapter();
			}
			@Override
			public Adapter caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object)
			{
				return createVersionedFunctionalModuleLogicAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object)
			{
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
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView <em>Aggregate Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView
	 * @generated
	 */
	public Adapter createAggregateEnrichmentViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView <em>Copy View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView
	 * @generated
	 */
	public Adapter createCopyViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView <em>Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView
	 * @generated
	 */
	public Adapter createEnrichmentViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView <em>Filter By Condition View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView
	 * @generated
	 */
	public Adapter createFilterByConditionViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView <em>Generic View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView
	 * @generated
	 */
	public Adapter createGenericViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause
	 * @generated
	 */
	public Adapter createGroupByClauseAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn <em>Ignore Column</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn
	 * @generated
	 */
	public Adapter createIgnoreColumnAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView <em>Join View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView
	 * @generated
	 */
	public Adapter createJoinViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs <em>Rename As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs
	 * @generated
	 */
	public Adapter createRenameAsAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause
	 * @generated
	 */
	public Adapter createSelectClauseAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView <em>SQL View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView
	 * @generated
	 */
	public Adapter createSQLViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView <em>Union View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView
	 * @generated
	 */
	public Adapter createUnionViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule <em>Versioned SQL Views Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule
	 * @generated
	 */
	public Adapter createVersionedSQLViewsModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause
	 * @generated
	 */
	public Adapter createWhereClauseAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas <em>Base View Incorporating Deltas</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas
	 * @generated
	 */
	public Adapter createBaseViewIncorporatingDeltasAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation <em>Delta Accumulation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation
	 * @generated
	 */
	public Adapter createDeltaAccumulationAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation <em>Data Set Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation
	 * @generated
	 */
	public Adapter createDataSetTransformationAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.common.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.common.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic <em>Versioned Functional Module Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic
	 * @generated
	 */
	public Adapter createVersionedFunctionalModuleLogicAdapter()
	{
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
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //Trl_sql_viewsAdapterFactory
