/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.impl;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.data_structures.smcubes.model.core.CorePackage;

import org.eclipse.efbt.data_structures.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.data_structures.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.data_structures.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.functional_design.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.functional_design.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.Domain_model_mappingPackageImpl;

import org.eclipse.efbt.language.trl.model.transformation.TransformationPackage;

import org.eclipse.efbt.language.trl.model.transformation.impl.TransformationPackageImpl;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl;

import org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas;
import org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation;
import org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause;
import org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn;
import org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs;
import org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsFactory;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;
import org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.lineage.common.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

import org.eclipse.efbt.requirements.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Trl_sql_viewsPackageImpl extends EPackageImpl implements Trl_sql_viewsPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateEnrichmentViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass copyViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass enrichmentViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterByConditionViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass genericViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass groupByClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass ignoreColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass joinViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass renameAsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass sqlViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unionViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass versionedSQLViewsModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass whereClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseViewIncorporatingDeltasEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass deltaAccumulationEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Trl_sql_viewsPackageImpl()
	{
		super(eNS_URI, Trl_sql_viewsFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Trl_sql_viewsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Trl_sql_viewsPackage init()
	{
		if (isInited) return (Trl_sql_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(Trl_sql_viewsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTrl_sql_viewsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Trl_sql_viewsPackageImpl theTrl_sql_viewsPackage = registeredTrl_sql_viewsPackage instanceof Trl_sql_viewsPackageImpl ? (Trl_sql_viewsPackageImpl)registeredTrl_sql_viewsPackage : new Trl_sql_viewsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		LineagefunctionsPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = (Domain_model_mappingPackageImpl)(registeredPackage instanceof Domain_model_mappingPackageImpl ? registeredPackage : Domain_model_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		TransformationPackageImpl theTransformationPackage = (TransformationPackageImpl)(registeredPackage instanceof TransformationPackageImpl ? registeredPackage : TransformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Trl_report_cell_viewsPackage.eNS_URI);
		Trl_report_cell_viewsPackageImpl theTrl_report_cell_viewsPackage = (Trl_report_cell_viewsPackageImpl)(registeredPackage instanceof Trl_report_cell_viewsPackageImpl ? registeredPackage : Trl_report_cell_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		theTrl_sql_viewsPackage.createPackageContents();
		theDomain_model_mappingPackage.createPackageContents();
		theTransformationPackage.createPackageContents();
		theTrl_report_cell_viewsPackage.createPackageContents();

		// Initialize created meta-data
		theTrl_sql_viewsPackage.initializePackageContents();
		theDomain_model_mappingPackage.initializePackageContents();
		theTransformationPackage.initializePackageContents();
		theTrl_report_cell_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTrl_sql_viewsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Trl_sql_viewsPackage.eNS_URI, theTrl_sql_viewsPackage);
		return theTrl_sql_viewsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAggregateEnrichmentView()
	{
		return aggregateEnrichmentViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentView_Functions()
	{
		return (EReference)aggregateEnrichmentViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentView_GroupByClause()
	{
		return (EReference)aggregateEnrichmentViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCopyView()
	{
		return copyViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getEnrichmentView()
	{
		return enrichmentViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEnrichmentView_Functions()
	{
		return (EReference)enrichmentViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEnrichmentView_Renames()
	{
		return (EReference)enrichmentViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFilterByConditionView()
	{
		return filterByConditionViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilterByConditionView_WhereClause()
	{
		return (EReference)filterByConditionViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGenericView()
	{
		return genericViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_SelectClause()
	{
		return (EReference)genericViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_WhereClause()
	{
		return (EReference)genericViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_GroupByClause()
	{
		return (EReference)genericViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGroupByClause()
	{
		return groupByClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGroupByClause_GroupByColumns()
	{
		return (EReference)groupByClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getIgnoreColumn()
	{
		return ignoreColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getIgnoreColumn_ColumnToIgnore()
	{
		return (EReference)ignoreColumnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getIgnoreColumn_Cube()
	{
		return (EReference)ignoreColumnEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getJoinView()
	{
		return joinViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_WhereClause()
	{
		return (EReference)joinViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_Renames()
	{
		return (EReference)joinViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_Ignores()
	{
		return (EReference)joinViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRenameAs()
	{
		return renameAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_FromCube()
	{
		return (EReference)renameAsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_FromVariable()
	{
		return (EReference)renameAsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_ToVariable()
	{
		return (EReference)renameAsEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectClause()
	{
		return selectClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_AggregateFunctions()
	{
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_BasicFunctions()
	{
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_Varaibles()
	{
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSQLView()
	{
		return sqlViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSQLView_VTL_Code()
	{
		return (EAttribute)sqlViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUnionView()
	{
		return unionViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVersionedSQLViewsModule()
	{
		return versionedSQLViewsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedSQLViewsModule_SqlViews()
	{
		return (EReference)versionedSQLViewsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getWhereClause()
	{
		return whereClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getWhereClause_Function()
	{
		return (EReference)whereClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseViewIncorporatingDeltas()
	{
		return baseViewIncorporatingDeltasEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseViewIncorporatingDeltas_UltimateSourceCube()
	{
		return (EReference)baseViewIncorporatingDeltasEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDeltaAccumulation()
	{
		return deltaAccumulationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDeltaAccumulation_UltimateSourceCube()
	{
		return (EReference)deltaAccumulationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Trl_sql_viewsFactory getTrl_sql_viewsFactory()
	{
		return (Trl_sql_viewsFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents()
	{
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		aggregateEnrichmentViewEClass = createEClass(AGGREGATE_ENRICHMENT_VIEW);
		createEReference(aggregateEnrichmentViewEClass, AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS);
		createEReference(aggregateEnrichmentViewEClass, AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE);

		copyViewEClass = createEClass(COPY_VIEW);

		enrichmentViewEClass = createEClass(ENRICHMENT_VIEW);
		createEReference(enrichmentViewEClass, ENRICHMENT_VIEW__FUNCTIONS);
		createEReference(enrichmentViewEClass, ENRICHMENT_VIEW__RENAMES);

		filterByConditionViewEClass = createEClass(FILTER_BY_CONDITION_VIEW);
		createEReference(filterByConditionViewEClass, FILTER_BY_CONDITION_VIEW__WHERE_CLAUSE);

		genericViewEClass = createEClass(GENERIC_VIEW);
		createEReference(genericViewEClass, GENERIC_VIEW__SELECT_CLAUSE);
		createEReference(genericViewEClass, GENERIC_VIEW__WHERE_CLAUSE);
		createEReference(genericViewEClass, GENERIC_VIEW__GROUP_BY_CLAUSE);

		groupByClauseEClass = createEClass(GROUP_BY_CLAUSE);
		createEReference(groupByClauseEClass, GROUP_BY_CLAUSE__GROUP_BY_COLUMNS);

		ignoreColumnEClass = createEClass(IGNORE_COLUMN);
		createEReference(ignoreColumnEClass, IGNORE_COLUMN__COLUMN_TO_IGNORE);
		createEReference(ignoreColumnEClass, IGNORE_COLUMN__CUBE);

		joinViewEClass = createEClass(JOIN_VIEW);
		createEReference(joinViewEClass, JOIN_VIEW__WHERE_CLAUSE);
		createEReference(joinViewEClass, JOIN_VIEW__RENAMES);
		createEReference(joinViewEClass, JOIN_VIEW__IGNORES);

		renameAsEClass = createEClass(RENAME_AS);
		createEReference(renameAsEClass, RENAME_AS__FROM_CUBE);
		createEReference(renameAsEClass, RENAME_AS__FROM_VARIABLE);
		createEReference(renameAsEClass, RENAME_AS__TO_VARIABLE);

		selectClauseEClass = createEClass(SELECT_CLAUSE);
		createEReference(selectClauseEClass, SELECT_CLAUSE__AGGREGATE_FUNCTIONS);
		createEReference(selectClauseEClass, SELECT_CLAUSE__BASIC_FUNCTIONS);
		createEReference(selectClauseEClass, SELECT_CLAUSE__VARAIBLES);

		sqlViewEClass = createEClass(SQL_VIEW);
		createEAttribute(sqlViewEClass, SQL_VIEW__VTL_CODE);

		unionViewEClass = createEClass(UNION_VIEW);

		versionedSQLViewsModuleEClass = createEClass(VERSIONED_SQL_VIEWS_MODULE);
		createEReference(versionedSQLViewsModuleEClass, VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS);

		whereClauseEClass = createEClass(WHERE_CLAUSE);
		createEReference(whereClauseEClass, WHERE_CLAUSE__FUNCTION);

		baseViewIncorporatingDeltasEClass = createEClass(BASE_VIEW_INCORPORATING_DELTAS);
		createEReference(baseViewIncorporatingDeltasEClass, BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE);

		deltaAccumulationEClass = createEClass(DELTA_ACCUMULATION);
		createEReference(deltaAccumulationEClass, DELTA_ACCUMULATION__ULTIMATE_SOURCE_CUBE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents()
	{
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Column_transformation_logicPackage theColumn_transformation_logicPackage = (Column_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		TransformationPackage theTransformationPackage = (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		aggregateEnrichmentViewEClass.getESuperTypes().add(this.getSQLView());
		copyViewEClass.getESuperTypes().add(this.getSQLView());
		enrichmentViewEClass.getESuperTypes().add(this.getSQLView());
		filterByConditionViewEClass.getESuperTypes().add(this.getSQLView());
		genericViewEClass.getESuperTypes().add(this.getSQLView());
		joinViewEClass.getESuperTypes().add(this.getSQLView());
		sqlViewEClass.getESuperTypes().add(theTransformationPackage.getDataSetTransformation());
		unionViewEClass.getESuperTypes().add(this.getSQLView());
		versionedSQLViewsModuleEClass.getESuperTypes().add(theTransformationPackage.getVersionedFunctionalModuleLogic());
		baseViewIncorporatingDeltasEClass.getESuperTypes().add(this.getSQLView());
		deltaAccumulationEClass.getESuperTypes().add(this.getSQLView());

		// Initialize classes, features, and operations; add parameters
		initEClass(aggregateEnrichmentViewEClass, AggregateEnrichmentView.class, "AggregateEnrichmentView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateEnrichmentView_Functions(), theColumn_transformation_logicPackage.getAggregateColumnFunction(), null, "functions", null, 0, -1, AggregateEnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAggregateEnrichmentView_GroupByClause(), this.getGroupByClause(), null, "groupByClause", null, 0, 1, AggregateEnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(copyViewEClass, CopyView.class, "CopyView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(enrichmentViewEClass, EnrichmentView.class, "EnrichmentView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getEnrichmentView_Functions(), theColumn_transformation_logicPackage.getBasicColumnFunction(), null, "functions", null, 0, -1, EnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getEnrichmentView_Renames(), this.getRenameAs(), null, "renames", null, 0, -1, EnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(filterByConditionViewEClass, FilterByConditionView.class, "FilterByConditionView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterByConditionView_WhereClause(), this.getWhereClause(), null, "whereClause", null, 0, 1, FilterByConditionView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(genericViewEClass, GenericView.class, "GenericView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGenericView_SelectClause(), this.getSelectClause(), null, "selectClause", null, 0, 1, GenericView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGenericView_WhereClause(), this.getWhereClause(), null, "whereClause", null, 0, 1, GenericView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGenericView_GroupByClause(), this.getGroupByClause(), null, "groupByClause", null, 0, 1, GenericView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(groupByClauseEClass, GroupByClause.class, "GroupByClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGroupByClause_GroupByColumns(), theCorePackage.getVARIABLE(), null, "groupByColumns", null, 0, -1, GroupByClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(ignoreColumnEClass, IgnoreColumn.class, "IgnoreColumn", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getIgnoreColumn_ColumnToIgnore(), theCorePackage.getVARIABLE(), null, "columnToIgnore", null, 0, 1, IgnoreColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getIgnoreColumn_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, IgnoreColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(joinViewEClass, JoinView.class, "JoinView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getJoinView_WhereClause(), this.getWhereClause(), null, "whereClause", null, 0, 1, JoinView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getJoinView_Renames(), this.getRenameAs(), null, "renames", null, 0, -1, JoinView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getJoinView_Ignores(), this.getIgnoreColumn(), null, "ignores", null, 0, -1, JoinView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(renameAsEClass, RenameAs.class, "RenameAs", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRenameAs_FromCube(), theCubesPackage.getFreeBirdToolsCube(), null, "fromCube", null, 0, 1, RenameAs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRenameAs_FromVariable(), theCorePackage.getVARIABLE(), null, "fromVariable", null, 0, 1, RenameAs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRenameAs_ToVariable(), theCorePackage.getVARIABLE(), null, "toVariable", null, 0, 1, RenameAs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(selectClauseEClass, SelectClause.class, "SelectClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSelectClause_AggregateFunctions(), theFunctionsPackage.getAggregateFunction(), null, "aggregateFunctions", null, 0, -1, SelectClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSelectClause_BasicFunctions(), theFunctionsPackage.getBasicFunction(), null, "basicFunctions", null, 0, 1, SelectClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSelectClause_Varaibles(), theCorePackage.getVARIABLE(), null, "varaibles", null, 0, -1, SelectClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(sqlViewEClass, SQLView.class, "SQLView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getSQLView_VTL_Code(), ecorePackage.getEString(), "VTL_Code", null, 0, 1, SQLView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(unionViewEClass, UnionView.class, "UnionView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(versionedSQLViewsModuleEClass, VersionedSQLViewsModule.class, "VersionedSQLViewsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVersionedSQLViewsModule_SqlViews(), this.getSQLView(), null, "sqlViews", null, 0, -1, VersionedSQLViewsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(whereClauseEClass, WhereClause.class, "WhereClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getWhereClause_Function(), theFunctionsPackage.getBooleanFunction(), null, "function", null, 0, 1, WhereClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseViewIncorporatingDeltasEClass, BaseViewIncorporatingDeltas.class, "BaseViewIncorporatingDeltas", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseViewIncorporatingDeltas_UltimateSourceCube(), theCubesPackage.getFreeBirdToolsCube(), null, "ultimateSourceCube", null, 0, 1, BaseViewIncorporatingDeltas.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(deltaAccumulationEClass, DeltaAccumulation.class, "DeltaAccumulation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDeltaAccumulation_UltimateSourceCube(), theCubesPackage.getFreeBirdToolsCube(), null, "ultimateSourceCube", null, 0, 1, DeltaAccumulation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations()
	{
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[]
		   {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations()
	{
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (aggregateEnrichmentViewEClass,
		   source,
		   new String[]
		   {
			   "name", "AggregateEnrichmentView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateEnrichmentView_Functions(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "functions"
		   });
		addAnnotation
		  (getAggregateEnrichmentView_GroupByClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "groupByClause"
		   });
		addAnnotation
		  (copyViewEClass,
		   source,
		   new String[]
		   {
			   "name", "CopyView",
			   "kind", "empty"
		   });
		addAnnotation
		  (enrichmentViewEClass,
		   source,
		   new String[]
		   {
			   "name", "EnrichmentView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getEnrichmentView_Functions(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "functions"
		   });
		addAnnotation
		  (getEnrichmentView_Renames(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "renames"
		   });
		addAnnotation
		  (filterByConditionViewEClass,
		   source,
		   new String[]
		   {
			   "name", "FilterByConditionView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterByConditionView_WhereClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (genericViewEClass,
		   source,
		   new String[]
		   {
			   "name", "GenericView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getGenericView_SelectClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "selectClause"
		   });
		addAnnotation
		  (getGenericView_WhereClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (getGenericView_GroupByClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "groupByClause"
		   });
		addAnnotation
		  (groupByClauseEClass,
		   source,
		   new String[]
		   {
			   "name", "GroupByClause",
			   "kind", "empty"
		   });
		addAnnotation
		  (getGroupByClause_GroupByColumns(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "groupByColumns"
		   });
		addAnnotation
		  (ignoreColumnEClass,
		   source,
		   new String[]
		   {
			   "name", "IgnoreColumn",
			   "kind", "empty"
		   });
		addAnnotation
		  (getIgnoreColumn_ColumnToIgnore(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "columnToIgnore"
		   });
		addAnnotation
		  (getIgnoreColumn_Cube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (joinViewEClass,
		   source,
		   new String[]
		   {
			   "name", "JoinView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getJoinView_WhereClause(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (getJoinView_Renames(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "renames"
		   });
		addAnnotation
		  (getJoinView_Ignores(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "ignores"
		   });
		addAnnotation
		  (renameAsEClass,
		   source,
		   new String[]
		   {
			   "name", "RenameAs",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRenameAs_FromCube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "fromCube"
		   });
		addAnnotation
		  (getRenameAs_FromVariable(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "fromVariable"
		   });
		addAnnotation
		  (getRenameAs_ToVariable(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "toVariable"
		   });
		addAnnotation
		  (selectClauseEClass,
		   source,
		   new String[]
		   {
			   "name", "SelectClause",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSelectClause_AggregateFunctions(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "aggregateFunctions"
		   });
		addAnnotation
		  (getSelectClause_BasicFunctions(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "basicFunctions"
		   });
		addAnnotation
		  (getSelectClause_Varaibles(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "varaibles"
		   });
		addAnnotation
		  (sqlViewEClass,
		   source,
		   new String[]
		   {
			   "name", "SQLView",
			   "kind", "empty"
		   });
		addAnnotation
		  (unionViewEClass,
		   source,
		   new String[]
		   {
			   "name", "UnionView",
			   "kind", "empty"
		   });
		addAnnotation
		  (versionedSQLViewsModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "VersionedSQLViewsModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getVersionedSQLViewsModule_SqlViews(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "sqlViews"
		   });
		addAnnotation
		  (whereClauseEClass,
		   source,
		   new String[]
		   {
			   "name", "WhereClause",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getWhereClause_Function(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "function"
		   });
	}

} //Trl_sql_viewsPackageImpl
