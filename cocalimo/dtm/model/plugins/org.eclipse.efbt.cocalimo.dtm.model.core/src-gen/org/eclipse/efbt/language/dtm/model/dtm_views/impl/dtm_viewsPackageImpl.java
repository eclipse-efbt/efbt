/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl.Domain_model_mappingPackageImpl;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.dtm_report_cell_viewsPackageImpl;

import org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView;
import org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy;
import org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_views.CopyView;
import org.eclipse.efbt.language.dtm.model.dtm_views.DTMView;
import org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView;
import org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView;
import org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView;
import org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView;
import org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_views.GenericView;
import org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause;
import org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn;
import org.eclipse.efbt.language.dtm.model.dtm_views.JoinView;
import org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView;
import org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView;
import org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs;
import org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause;
import org.eclipse.efbt.language.dtm.model.dtm_views.UnionView;
import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;
import org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;

import org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl;

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
public class dtm_viewsPackageImpl extends EPackageImpl implements dtm_viewsPackage {
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
	private EClass dtmViewEClass = null;

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
	private EClass versionedDTMViewsModuleEClass = null;

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
	private EClass castColumnViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass explodeArrayOfStructsViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass explodeStructViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterByStructClassColumnViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass makeArrayOfStructsViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass makeStructViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateEnrichmentViewAndOrderByEClass = null;

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
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private dtm_viewsPackageImpl() {
		super(eNS_URI, dtm_viewsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link dtm_viewsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static dtm_viewsPackage init() {
		if (isInited) return (dtm_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(dtm_viewsPackage.eNS_URI);

		// Obtain or create and register package
		Object registereddtm_viewsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		dtm_viewsPackageImpl thedtm_viewsPackage = registereddtm_viewsPackage instanceof dtm_viewsPackageImpl ? (dtm_viewsPackageImpl)registereddtm_viewsPackage : new dtm_viewsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		ProgramPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		Efbt_data_definitionPackage.eINSTANCE.eClass();
		Efbt_mappingPackage.eINSTANCE.eClass();
		Efbt_vtl_transformationPackage.eINSTANCE.eClass();
		MappingPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();
		Smcubes_modelPackage.eINSTANCE.eClass();
		Vtl_transformationPackage.eINSTANCE.eClass();
		Test_definitionPackage.eINSTANCE.eClass();
		Test_input_dataPackage.eINSTANCE.eClass();
		TestPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = (Domain_model_mappingPackageImpl)(registeredPackage instanceof Domain_model_mappingPackageImpl ? registeredPackage : Domain_model_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		TransformationPackageImpl theTransformationPackage = (TransformationPackageImpl)(registeredPackage instanceof TransformationPackageImpl ? registeredPackage : TransformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(dtm_report_cell_viewsPackage.eNS_URI);
		dtm_report_cell_viewsPackageImpl thedtm_report_cell_viewsPackage = (dtm_report_cell_viewsPackageImpl)(registeredPackage instanceof dtm_report_cell_viewsPackageImpl ? registeredPackage : dtm_report_cell_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		thedtm_viewsPackage.createPackageContents();
		theDomain_model_mappingPackage.createPackageContents();
		theTransformationPackage.createPackageContents();
		thedtm_report_cell_viewsPackage.createPackageContents();

		// Initialize created meta-data
		thedtm_viewsPackage.initializePackageContents();
		theDomain_model_mappingPackage.initializePackageContents();
		theTransformationPackage.initializePackageContents();
		thedtm_report_cell_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		thedtm_viewsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(dtm_viewsPackage.eNS_URI, thedtm_viewsPackage);
		return thedtm_viewsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAggregateEnrichmentView() {
		return aggregateEnrichmentViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentView_Functions() {
		return (EReference)aggregateEnrichmentViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentView_GroupByClause() {
		return (EReference)aggregateEnrichmentViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCopyView() {
		return copyViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getEnrichmentView() {
		return enrichmentViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEnrichmentView_Functions() {
		return (EReference)enrichmentViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEnrichmentView_Renames() {
		return (EReference)enrichmentViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEnrichmentView_Ignores() {
		return (EReference)enrichmentViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFilterByConditionView() {
		return filterByConditionViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilterByConditionView_WhereClause() {
		return (EReference)filterByConditionViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGenericView() {
		return genericViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_SelectClause() {
		return (EReference)genericViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_WhereClause() {
		return (EReference)genericViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenericView_GroupByClause() {
		return (EReference)genericViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGroupByClause() {
		return groupByClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGroupByClause_GroupByColumns() {
		return (EReference)groupByClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getIgnoreColumn() {
		return ignoreColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getIgnoreColumn_ColumnToIgnore() {
		return (EReference)ignoreColumnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getIgnoreColumn_Cube() {
		return (EReference)ignoreColumnEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getJoinView() {
		return joinViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_WhereClause() {
		return (EReference)joinViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_Renames() {
		return (EReference)joinViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getJoinView_Ignores() {
		return (EReference)joinViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRenameAs() {
		return renameAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_FromCube() {
		return (EReference)renameAsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_FromVariable() {
		return (EReference)renameAsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRenameAs_ToVariable() {
		return (EReference)renameAsEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectClause() {
		return selectClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_AggregateFunctions() {
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_BasicFunctions() {
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectClause_Varaibles() {
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDTMView() {
		return dtmViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUnionView() {
		return unionViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVersionedDTMViewsModule() {
		return versionedDTMViewsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedDTMViewsModule_DTMViews() {
		return (EReference)versionedDTMViewsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getWhereClause() {
		return whereClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getWhereClause_Function() {
		return (EReference)whereClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCastColumnView() {
		return castColumnViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCastColumnView_SourceStructVariable() {
		return (EReference)castColumnViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCastColumnView_TargetStructVariable() {
		return (EReference)castColumnViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getExplodeArrayOfStructsView() {
		return explodeArrayOfStructsViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExplodeArrayOfStructsView_ArraySourceColumn() {
		return (EReference)explodeArrayOfStructsViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExplodeArrayOfStructsView_IndexColumn() {
		return (EReference)explodeArrayOfStructsViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExplodeArrayOfStructsView_TargetStructColumn() {
		return (EReference)explodeArrayOfStructsViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getExplodeStructView() {
		return explodeStructViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExplodeStructView_ColumnToUnpack() {
		return (EReference)explodeStructViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFilterByStructClassColumnView() {
		return filterByStructClassColumnViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilterByStructClassColumnView_RenameAsStructColumn() {
		return (EReference)filterByStructClassColumnViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilterByStructClassColumnView_StructClass() {
		return (EReference)filterByStructClassColumnViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilterByStructClassColumnView_StructColumn() {
		return (EReference)filterByStructClassColumnViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMakeArrayOfStructsView() {
		return makeArrayOfStructsViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMakeArrayOfStructsView_GroupingVariable() {
		return (EReference)makeArrayOfStructsViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMakeArrayOfStructsView_SourceStructVariable() {
		return (EReference)makeArrayOfStructsViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMakeArrayOfStructsView_TargetArrayVariable() {
		return (EReference)makeArrayOfStructsViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMakeStructView() {
		return makeStructViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMakeStructView_TargetVariable() {
		return (EReference)makeStructViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAggregateEnrichmentViewAndOrderBy() {
		return aggregateEnrichmentViewAndOrderByEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentViewAndOrderBy_Functions() {
		return (EReference)aggregateEnrichmentViewAndOrderByEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentViewAndOrderBy_GroupByClause() {
		return (EReference)aggregateEnrichmentViewAndOrderByEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateEnrichmentViewAndOrderBy_OrderByVariable() {
		return (EReference)aggregateEnrichmentViewAndOrderByEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public dtm_viewsFactory getdtm_viewsFactory() {
		return (dtm_viewsFactory)getEFactoryInstance();
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
	public void createPackageContents() {
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
		createEReference(enrichmentViewEClass, ENRICHMENT_VIEW__IGNORES);

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

		dtmViewEClass = createEClass(DTM_VIEW);

		unionViewEClass = createEClass(UNION_VIEW);

		versionedDTMViewsModuleEClass = createEClass(VERSIONED_DTM_VIEWS_MODULE);
		createEReference(versionedDTMViewsModuleEClass, VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS);

		whereClauseEClass = createEClass(WHERE_CLAUSE);
		createEReference(whereClauseEClass, WHERE_CLAUSE__FUNCTION);

		castColumnViewEClass = createEClass(CAST_COLUMN_VIEW);
		createEReference(castColumnViewEClass, CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE);
		createEReference(castColumnViewEClass, CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE);

		explodeArrayOfStructsViewEClass = createEClass(EXPLODE_ARRAY_OF_STRUCTS_VIEW);
		createEReference(explodeArrayOfStructsViewEClass, EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN);
		createEReference(explodeArrayOfStructsViewEClass, EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN);
		createEReference(explodeArrayOfStructsViewEClass, EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN);

		explodeStructViewEClass = createEClass(EXPLODE_STRUCT_VIEW);
		createEReference(explodeStructViewEClass, EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK);

		filterByStructClassColumnViewEClass = createEClass(FILTER_BY_STRUCT_CLASS_COLUMN_VIEW);
		createEReference(filterByStructClassColumnViewEClass, FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN);
		createEReference(filterByStructClassColumnViewEClass, FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS);
		createEReference(filterByStructClassColumnViewEClass, FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN);

		makeArrayOfStructsViewEClass = createEClass(MAKE_ARRAY_OF_STRUCTS_VIEW);
		createEReference(makeArrayOfStructsViewEClass, MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE);
		createEReference(makeArrayOfStructsViewEClass, MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE);
		createEReference(makeArrayOfStructsViewEClass, MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE);

		makeStructViewEClass = createEClass(MAKE_STRUCT_VIEW);
		createEReference(makeStructViewEClass, MAKE_STRUCT_VIEW__TARGET_VARIABLE);

		aggregateEnrichmentViewAndOrderByEClass = createEClass(AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY);
		createEReference(aggregateEnrichmentViewAndOrderByEClass, AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS);
		createEReference(aggregateEnrichmentViewAndOrderByEClass, AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE);
		createEReference(aggregateEnrichmentViewAndOrderByEClass, AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE);
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
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		TransformationPackage theTransformationPackage = (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		Efbt_advanced_data_definitionPackage theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		aggregateEnrichmentViewEClass.getESuperTypes().add(this.getDTMView());
		copyViewEClass.getESuperTypes().add(this.getDTMView());
		enrichmentViewEClass.getESuperTypes().add(this.getDTMView());
		filterByConditionViewEClass.getESuperTypes().add(this.getDTMView());
		genericViewEClass.getESuperTypes().add(this.getDTMView());
		joinViewEClass.getESuperTypes().add(this.getDTMView());
		dtmViewEClass.getESuperTypes().add(theTransformationPackage.getDataSetTransformation());
		unionViewEClass.getESuperTypes().add(this.getDTMView());
		versionedDTMViewsModuleEClass.getESuperTypes().add(theTransformationPackage.getVersionedFunctionalModuleLogic());
		castColumnViewEClass.getESuperTypes().add(this.getDTMView());
		explodeArrayOfStructsViewEClass.getESuperTypes().add(this.getDTMView());
		explodeStructViewEClass.getESuperTypes().add(this.getDTMView());
		filterByStructClassColumnViewEClass.getESuperTypes().add(this.getDTMView());
		makeArrayOfStructsViewEClass.getESuperTypes().add(this.getDTMView());
		makeStructViewEClass.getESuperTypes().add(this.getDTMView());
		aggregateEnrichmentViewAndOrderByEClass.getESuperTypes().add(this.getDTMView());

		// Initialize classes, features, and operations; add parameters
		initEClass(aggregateEnrichmentViewEClass, AggregateEnrichmentView.class, "AggregateEnrichmentView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateEnrichmentView_Functions(), theFunctionsPackage.getAggregateColumnFunction(), null, "functions", null, 0, -1, AggregateEnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAggregateEnrichmentView_GroupByClause(), this.getGroupByClause(), null, "groupByClause", null, 0, 1, AggregateEnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(copyViewEClass, CopyView.class, "CopyView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(enrichmentViewEClass, EnrichmentView.class, "EnrichmentView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getEnrichmentView_Functions(), theFunctionsPackage.getBasicColumnFunction(), null, "functions", null, 0, -1, EnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getEnrichmentView_Renames(), this.getRenameAs(), null, "renames", null, 0, -1, EnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getEnrichmentView_Ignores(), this.getIgnoreColumn(), null, "ignores", null, 0, -1, EnrichmentView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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

		initEClass(dtmViewEClass, DTMView.class, "DTMView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(unionViewEClass, UnionView.class, "UnionView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(versionedDTMViewsModuleEClass, VersionedDTMViewsModule.class, "VersionedDTMViewsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVersionedDTMViewsModule_DTMViews(), this.getDTMView(), null, "DTMViews", null, 0, -1, VersionedDTMViewsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(whereClauseEClass, WhereClause.class, "WhereClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getWhereClause_Function(), theFunctionsPackage.getBooleanFunction(), null, "function", null, 0, 1, WhereClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(castColumnViewEClass, CastColumnView.class, "CastColumnView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCastColumnView_SourceStructVariable(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "sourceStructVariable", null, 0, 1, CastColumnView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCastColumnView_TargetStructVariable(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "targetStructVariable", null, 0, 1, CastColumnView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(explodeArrayOfStructsViewEClass, ExplodeArrayOfStructsView.class, "ExplodeArrayOfStructsView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExplodeArrayOfStructsView_ArraySourceColumn(), theEfbt_advanced_data_definitionPackage.getArrayTypedVariable(), null, "arraySourceColumn", null, 0, 1, ExplodeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getExplodeArrayOfStructsView_IndexColumn(), theCorePackage.getVARIABLE(), null, "indexColumn", null, 0, 1, ExplodeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getExplodeArrayOfStructsView_TargetStructColumn(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "targetStructColumn", null, 0, 1, ExplodeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(explodeStructViewEClass, ExplodeStructView.class, "ExplodeStructView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExplodeStructView_ColumnToUnpack(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "columnToUnpack", null, 0, 1, ExplodeStructView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(filterByStructClassColumnViewEClass, FilterByStructClassColumnView.class, "FilterByStructClassColumnView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterByStructClassColumnView_RenameAsStructColumn(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "renameAsStructColumn", null, 0, 1, FilterByStructClassColumnView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFilterByStructClassColumnView_StructClass(), theCubesPackage.getFreeBirdToolsCube(), null, "structClass", null, 0, 1, FilterByStructClassColumnView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFilterByStructClassColumnView_StructColumn(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "structColumn", null, 0, 1, FilterByStructClassColumnView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(makeArrayOfStructsViewEClass, MakeArrayOfStructsView.class, "MakeArrayOfStructsView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMakeArrayOfStructsView_GroupingVariable(), theCorePackage.getVARIABLE(), null, "groupingVariable", null, 0, 1, MakeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMakeArrayOfStructsView_SourceStructVariable(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "sourceStructVariable", null, 0, 1, MakeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMakeArrayOfStructsView_TargetArrayVariable(), theEfbt_advanced_data_definitionPackage.getArrayTypedVariable(), null, "targetArrayVariable", null, 0, 1, MakeArrayOfStructsView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(makeStructViewEClass, MakeStructView.class, "MakeStructView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMakeStructView_TargetVariable(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "targetVariable", null, 0, 1, MakeStructView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateEnrichmentViewAndOrderByEClass, AggregateEnrichmentViewAndOrderBy.class, "AggregateEnrichmentViewAndOrderBy", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateEnrichmentViewAndOrderBy_Functions(), theFunctionsPackage.getAggregateColumnFunction(), null, "functions", null, 0, -1, AggregateEnrichmentViewAndOrderBy.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAggregateEnrichmentViewAndOrderBy_GroupByClause(), this.getGroupByClause(), null, "groupByClause", null, 0, 1, AggregateEnrichmentViewAndOrderBy.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAggregateEnrichmentViewAndOrderBy_OrderByVariable(), theCorePackage.getVARIABLE(), null, "orderByVariable", null, 0, 1, AggregateEnrichmentViewAndOrderBy.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations() {
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (aggregateEnrichmentViewEClass,
		   source,
		   new String[] {
			   "name", "AggregateEnrichmentView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateEnrichmentView_Functions(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "functions"
		   });
		addAnnotation
		  (getAggregateEnrichmentView_GroupByClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "groupByClause"
		   });
		addAnnotation
		  (copyViewEClass,
		   source,
		   new String[] {
			   "name", "CopyView",
			   "kind", "empty"
		   });
		addAnnotation
		  (enrichmentViewEClass,
		   source,
		   new String[] {
			   "name", "EnrichmentView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getEnrichmentView_Functions(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "functions"
		   });
		addAnnotation
		  (getEnrichmentView_Renames(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "renames"
		   });
		addAnnotation
		  (getEnrichmentView_Ignores(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "ignores"
		   });
		addAnnotation
		  (filterByConditionViewEClass,
		   source,
		   new String[] {
			   "name", "FilterByConditionView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterByConditionView_WhereClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (genericViewEClass,
		   source,
		   new String[] {
			   "name", "GenericView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getGenericView_SelectClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "selectClause"
		   });
		addAnnotation
		  (getGenericView_WhereClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (getGenericView_GroupByClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "groupByClause"
		   });
		addAnnotation
		  (groupByClauseEClass,
		   source,
		   new String[] {
			   "name", "GroupByClause",
			   "kind", "empty"
		   });
		addAnnotation
		  (getGroupByClause_GroupByColumns(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "groupByColumns"
		   });
		addAnnotation
		  (ignoreColumnEClass,
		   source,
		   new String[] {
			   "name", "IgnoreColumn",
			   "kind", "empty"
		   });
		addAnnotation
		  (getIgnoreColumn_ColumnToIgnore(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "columnToIgnore"
		   });
		addAnnotation
		  (getIgnoreColumn_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (joinViewEClass,
		   source,
		   new String[] {
			   "name", "JoinView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getJoinView_WhereClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "whereClause"
		   });
		addAnnotation
		  (getJoinView_Renames(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "renames"
		   });
		addAnnotation
		  (getJoinView_Ignores(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "ignores"
		   });
		addAnnotation
		  (renameAsEClass,
		   source,
		   new String[] {
			   "name", "RenameAs",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRenameAs_FromCube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "fromCube"
		   });
		addAnnotation
		  (getRenameAs_FromVariable(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "fromVariable"
		   });
		addAnnotation
		  (getRenameAs_ToVariable(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "toVariable"
		   });
		addAnnotation
		  (selectClauseEClass,
		   source,
		   new String[] {
			   "name", "SelectClause",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSelectClause_AggregateFunctions(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "aggregateFunctions"
		   });
		addAnnotation
		  (getSelectClause_BasicFunctions(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "basicFunctions"
		   });
		addAnnotation
		  (getSelectClause_Varaibles(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "varaibles"
		   });
		addAnnotation
		  (dtmViewEClass,
		   source,
		   new String[] {
			   "name", "DTMView",
			   "kind", "empty"
		   });
		addAnnotation
		  (unionViewEClass,
		   source,
		   new String[] {
			   "name", "UnionView",
			   "kind", "empty"
		   });
		addAnnotation
		  (versionedDTMViewsModuleEClass,
		   source,
		   new String[] {
			   "name", "VersionedDTMViewsModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getVersionedDTMViewsModule_DTMViews(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "DTMViews"
		   });
		addAnnotation
		  (whereClauseEClass,
		   source,
		   new String[] {
			   "name", "WhereClause",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getWhereClause_Function(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "function"
		   });
		addAnnotation
		  (aggregateEnrichmentViewAndOrderByEClass,
		   source,
		   new String[] {
			   "name", "AggregateEnrichmentView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateEnrichmentViewAndOrderBy_Functions(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "functions"
		   });
		addAnnotation
		  (getAggregateEnrichmentViewAndOrderBy_GroupByClause(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "groupByClause"
		   });
	}

} //dtm_viewsPackageImpl
