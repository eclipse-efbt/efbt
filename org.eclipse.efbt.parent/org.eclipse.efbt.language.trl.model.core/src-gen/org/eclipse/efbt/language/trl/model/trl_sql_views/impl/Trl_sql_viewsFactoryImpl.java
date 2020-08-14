/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.impl;

import org.eclipse.efbt.language.trl.model.trl_sql_views.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Trl_sql_viewsFactoryImpl extends EFactoryImpl implements Trl_sql_viewsFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Trl_sql_viewsFactory init()
	{
		try
		{
			Trl_sql_viewsFactory theTrl_sql_viewsFactory = (Trl_sql_viewsFactory)EPackage.Registry.INSTANCE.getEFactory(Trl_sql_viewsPackage.eNS_URI);
			if (theTrl_sql_viewsFactory != null)
			{
				return theTrl_sql_viewsFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Trl_sql_viewsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Trl_sql_viewsFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID())
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW: return createAggregateEnrichmentView();
			case Trl_sql_viewsPackage.COPY_VIEW: return createCopyView();
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW: return createEnrichmentView();
			case Trl_sql_viewsPackage.FILTER_BY_CONDITION_VIEW: return createFilterByConditionView();
			case Trl_sql_viewsPackage.GENERIC_VIEW: return createGenericView();
			case Trl_sql_viewsPackage.GROUP_BY_CLAUSE: return createGroupByClause();
			case Trl_sql_viewsPackage.IGNORE_COLUMN: return createIgnoreColumn();
			case Trl_sql_viewsPackage.JOIN_VIEW: return createJoinView();
			case Trl_sql_viewsPackage.RENAME_AS: return createRenameAs();
			case Trl_sql_viewsPackage.SELECT_CLAUSE: return createSelectClause();
			case Trl_sql_viewsPackage.SQL_VIEW: return createSQLView();
			case Trl_sql_viewsPackage.UNION_VIEW: return createUnionView();
			case Trl_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE: return createVersionedSQLViewsModule();
			case Trl_sql_viewsPackage.WHERE_CLAUSE: return createWhereClause();
			case Trl_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS: return createBaseViewIncorporatingDeltas();
			case Trl_sql_viewsPackage.DELTA_ACCUMULATION: return createDeltaAccumulation();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AggregateEnrichmentView createAggregateEnrichmentView()
	{
		AggregateEnrichmentViewImpl aggregateEnrichmentView = new AggregateEnrichmentViewImpl();
		return aggregateEnrichmentView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CopyView createCopyView()
	{
		CopyViewImpl copyView = new CopyViewImpl();
		return copyView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EnrichmentView createEnrichmentView()
	{
		EnrichmentViewImpl enrichmentView = new EnrichmentViewImpl();
		return enrichmentView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FilterByConditionView createFilterByConditionView()
	{
		FilterByConditionViewImpl filterByConditionView = new FilterByConditionViewImpl();
		return filterByConditionView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GenericView createGenericView()
	{
		GenericViewImpl genericView = new GenericViewImpl();
		return genericView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GroupByClause createGroupByClause()
	{
		GroupByClauseImpl groupByClause = new GroupByClauseImpl();
		return groupByClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public IgnoreColumn createIgnoreColumn()
	{
		IgnoreColumnImpl ignoreColumn = new IgnoreColumnImpl();
		return ignoreColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public JoinView createJoinView()
	{
		JoinViewImpl joinView = new JoinViewImpl();
		return joinView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RenameAs createRenameAs()
	{
		RenameAsImpl renameAs = new RenameAsImpl();
		return renameAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectClause createSelectClause()
	{
		SelectClauseImpl selectClause = new SelectClauseImpl();
		return selectClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SQLView createSQLView()
	{
		SQLViewImpl sqlView = new SQLViewImpl();
		return sqlView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public UnionView createUnionView()
	{
		UnionViewImpl unionView = new UnionViewImpl();
		return unionView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VersionedSQLViewsModule createVersionedSQLViewsModule()
	{
		VersionedSQLViewsModuleImpl versionedSQLViewsModule = new VersionedSQLViewsModuleImpl();
		return versionedSQLViewsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public WhereClause createWhereClause()
	{
		WhereClauseImpl whereClause = new WhereClauseImpl();
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseViewIncorporatingDeltas createBaseViewIncorporatingDeltas()
	{
		BaseViewIncorporatingDeltasImpl baseViewIncorporatingDeltas = new BaseViewIncorporatingDeltasImpl();
		return baseViewIncorporatingDeltas;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DeltaAccumulation createDeltaAccumulation()
	{
		DeltaAccumulationImpl deltaAccumulation = new DeltaAccumulationImpl();
		return deltaAccumulation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Trl_sql_viewsPackage getTrl_sql_viewsPackage()
	{
		return (Trl_sql_viewsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Trl_sql_viewsPackage getPackage()
	{
		return Trl_sql_viewsPackage.eINSTANCE;
	}

} //Trl_sql_viewsFactoryImpl
