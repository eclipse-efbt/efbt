/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl;

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

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.CodeCondition;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingFactory;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToReportCellViewModule;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.ScenarioToCodeConditions;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.dtm_report_cell_viewsPackageImpl;

import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl;

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
public class Domain_model_mappingPackageImpl extends EPackageImpl implements Domain_model_mappingPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass leafModuleToVersionedDTMViewsModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass leafModuleToVersionedCubeSchemaModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass leafModuleToReportCellViewModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass scenarioToCodeConditionsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass codeConditionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnSettingConditionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataSetFilterConditionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unionChoiceConditionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass domainMappingModuleEClass = null;

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
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Domain_model_mappingPackageImpl() {
		super(eNS_URI, Domain_model_mappingFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Domain_model_mappingPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Domain_model_mappingPackage init() {
		if (isInited) return (Domain_model_mappingPackage)EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredDomain_model_mappingPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = registeredDomain_model_mappingPackage instanceof Domain_model_mappingPackageImpl ? (Domain_model_mappingPackageImpl)registeredDomain_model_mappingPackage : new Domain_model_mappingPackageImpl();

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
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		TransformationPackageImpl theTransformationPackage = (TransformationPackageImpl)(registeredPackage instanceof TransformationPackageImpl ? registeredPackage : TransformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(dtm_report_cell_viewsPackage.eNS_URI);
		dtm_report_cell_viewsPackageImpl thedtm_report_cell_viewsPackage = (dtm_report_cell_viewsPackageImpl)(registeredPackage instanceof dtm_report_cell_viewsPackageImpl ? registeredPackage : dtm_report_cell_viewsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(dtm_viewsPackage.eNS_URI);
		dtm_viewsPackageImpl thedtm_viewsPackage = (dtm_viewsPackageImpl)(registeredPackage instanceof dtm_viewsPackageImpl ? registeredPackage : dtm_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		theDomain_model_mappingPackage.createPackageContents();
		theTransformationPackage.createPackageContents();
		thedtm_report_cell_viewsPackage.createPackageContents();
		thedtm_viewsPackage.createPackageContents();

		// Initialize created meta-data
		theDomain_model_mappingPackage.initializePackageContents();
		theTransformationPackage.initializePackageContents();
		thedtm_report_cell_viewsPackage.initializePackageContents();
		thedtm_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theDomain_model_mappingPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Domain_model_mappingPackage.eNS_URI, theDomain_model_mappingPackage);
		return theDomain_model_mappingPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLeafModuleToVersionedDTMViewsModule() {
		return leafModuleToVersionedDTMViewsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToVersionedDTMViewsModule_Scheme() {
		return (EReference)leafModuleToVersionedDTMViewsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToVersionedDTMViewsModule_Module() {
		return (EReference)leafModuleToVersionedDTMViewsModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLeafModuleToVersionedCubeSchemaModule() {
		return leafModuleToVersionedCubeSchemaModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToVersionedCubeSchemaModule_Scheme() {
		return (EReference)leafModuleToVersionedCubeSchemaModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToVersionedCubeSchemaModule_Module() {
		return (EReference)leafModuleToVersionedCubeSchemaModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLeafModuleToReportCellViewModule() {
		return leafModuleToReportCellViewModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToReportCellViewModule_Scheme() {
		return (EReference)leafModuleToReportCellViewModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLeafModuleToReportCellViewModule_Module() {
		return (EReference)leafModuleToReportCellViewModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getScenarioToCodeConditions() {
		return scenarioToCodeConditionsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getScenarioToCodeConditions_Scenario() {
		return (EReference)scenarioToCodeConditionsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getScenarioToCodeConditions_CodeConditions() {
		return (EReference)scenarioToCodeConditionsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCodeCondition() {
		return codeConditionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnSettingCondition() {
		return columnSettingConditionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnSettingCondition_Cube() {
		return (EReference)columnSettingConditionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnSettingCondition_Variable() {
		return (EReference)columnSettingConditionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnSettingCondition_FilterConditions() {
		return (EReference)columnSettingConditionEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDataSetFilterCondition() {
		return dataSetFilterConditionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDataSetFilterCondition_FilterConditions() {
		return (EReference)dataSetFilterConditionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDataSetFilterCondition_Cube() {
		return (EReference)dataSetFilterConditionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUnionChoiceCondition() {
		return unionChoiceConditionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getUnionChoiceCondition_Cube() {
		return (EReference)unionChoiceConditionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getUnionChoiceCondition_ChosenCubes() {
		return (EReference)unionChoiceConditionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDomainMappingModule() {
		return domainMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainMappingModule_LeafModuleToVersionedDTMViewsModules() {
		return (EReference)domainMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainMappingModule_LeafModuleToVersionedCubeSchemaModules() {
		return (EReference)domainMappingModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainMappingModule_LeafModuleToReportCellViewModules() {
		return (EReference)domainMappingModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainMappingModule_ScenarioToCodeConditions() {
		return (EReference)domainMappingModuleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Domain_model_mappingFactory getDomain_model_mappingFactory() {
		return (Domain_model_mappingFactory)getEFactoryInstance();
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
		leafModuleToVersionedDTMViewsModuleEClass = createEClass(LEAF_MODULE_TO_VERSIONED_DTM_VIEWS_MODULE);
		createEReference(leafModuleToVersionedDTMViewsModuleEClass, LEAF_MODULE_TO_VERSIONED_DTM_VIEWS_MODULE__SCHEME);
		createEReference(leafModuleToVersionedDTMViewsModuleEClass, LEAF_MODULE_TO_VERSIONED_DTM_VIEWS_MODULE__MODULE);

		leafModuleToVersionedCubeSchemaModuleEClass = createEClass(LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULE);
		createEReference(leafModuleToVersionedCubeSchemaModuleEClass, LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULE__SCHEME);
		createEReference(leafModuleToVersionedCubeSchemaModuleEClass, LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULE__MODULE);

		leafModuleToReportCellViewModuleEClass = createEClass(LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE);
		createEReference(leafModuleToReportCellViewModuleEClass, LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME);
		createEReference(leafModuleToReportCellViewModuleEClass, LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE);

		scenarioToCodeConditionsEClass = createEClass(SCENARIO_TO_CODE_CONDITIONS);
		createEReference(scenarioToCodeConditionsEClass, SCENARIO_TO_CODE_CONDITIONS__SCENARIO);
		createEReference(scenarioToCodeConditionsEClass, SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS);

		codeConditionEClass = createEClass(CODE_CONDITION);

		columnSettingConditionEClass = createEClass(COLUMN_SETTING_CONDITION);
		createEReference(columnSettingConditionEClass, COLUMN_SETTING_CONDITION__CUBE);
		createEReference(columnSettingConditionEClass, COLUMN_SETTING_CONDITION__VARIABLE);
		createEReference(columnSettingConditionEClass, COLUMN_SETTING_CONDITION__FILTER_CONDITIONS);

		dataSetFilterConditionEClass = createEClass(DATA_SET_FILTER_CONDITION);
		createEReference(dataSetFilterConditionEClass, DATA_SET_FILTER_CONDITION__FILTER_CONDITIONS);
		createEReference(dataSetFilterConditionEClass, DATA_SET_FILTER_CONDITION__CUBE);

		unionChoiceConditionEClass = createEClass(UNION_CHOICE_CONDITION);
		createEReference(unionChoiceConditionEClass, UNION_CHOICE_CONDITION__CUBE);
		createEReference(unionChoiceConditionEClass, UNION_CHOICE_CONDITION__CHOSEN_CUBES);

		domainMappingModuleEClass = createEClass(DOMAIN_MAPPING_MODULE);
		createEReference(domainMappingModuleEClass, DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_DTM_VIEWS_MODULES);
		createEReference(domainMappingModuleEClass, DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES);
		createEReference(domainMappingModuleEClass, DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES);
		createEReference(domainMappingModuleEClass, DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS);
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
		Functionality_modulePackage theFunctionality_modulePackage = (Functionality_modulePackage)EPackage.Registry.INSTANCE.getEPackage(Functionality_modulePackage.eNS_URI);
		dtm_viewsPackage thedtm_viewsPackage = (dtm_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(dtm_viewsPackage.eNS_URI);
		TransformationPackage theTransformationPackage = (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		dtm_report_cell_viewsPackage thedtm_report_cell_viewsPackage = (dtm_report_cell_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(dtm_report_cell_viewsPackage.eNS_URI);
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		columnSettingConditionEClass.getESuperTypes().add(this.getCodeCondition());
		dataSetFilterConditionEClass.getESuperTypes().add(this.getCodeCondition());
		unionChoiceConditionEClass.getESuperTypes().add(this.getCodeCondition());
		domainMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(leafModuleToVersionedDTMViewsModuleEClass, LeafModuleToVersionedDTMViewsModule.class, "LeafModuleToVersionedDTMViewsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLeafModuleToVersionedDTMViewsModule_Scheme(), theFunctionality_modulePackage.getLeafFunctionalityModule(), null, "scheme", null, 0, 1, LeafModuleToVersionedDTMViewsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLeafModuleToVersionedDTMViewsModule_Module(), thedtm_viewsPackage.getVersionedDTMViewsModule(), null, "module", null, 0, 1, LeafModuleToVersionedDTMViewsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(leafModuleToVersionedCubeSchemaModuleEClass, LeafModuleToVersionedCubeSchemaModule.class, "LeafModuleToVersionedCubeSchemaModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLeafModuleToVersionedCubeSchemaModule_Scheme(), theFunctionality_modulePackage.getLeafFunctionalityModule(), null, "scheme", null, 0, 1, LeafModuleToVersionedCubeSchemaModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLeafModuleToVersionedCubeSchemaModule_Module(), theTransformationPackage.getVersionedCubeSchemaModule(), null, "module", null, 0, 1, LeafModuleToVersionedCubeSchemaModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(leafModuleToReportCellViewModuleEClass, LeafModuleToReportCellViewModule.class, "LeafModuleToReportCellViewModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLeafModuleToReportCellViewModule_Scheme(), theFunctionality_modulePackage.getLeafFunctionalityModule(), null, "scheme", null, 0, 1, LeafModuleToReportCellViewModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLeafModuleToReportCellViewModule_Module(), thedtm_report_cell_viewsPackage.getReportCellViewModule(), null, "module", null, 0, 1, LeafModuleToReportCellViewModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(scenarioToCodeConditionsEClass, ScenarioToCodeConditions.class, "ScenarioToCodeConditions", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getScenarioToCodeConditions_Scenario(), theScenariosPackage.getScenario(), null, "scenario", null, 0, 1, ScenarioToCodeConditions.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getScenarioToCodeConditions_CodeConditions(), this.getCodeCondition(), null, "codeConditions", null, 0, -1, ScenarioToCodeConditions.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(codeConditionEClass, CodeCondition.class, "CodeCondition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(columnSettingConditionEClass, ColumnSettingCondition.class, "ColumnSettingCondition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumnSettingCondition_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, ColumnSettingCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getColumnSettingCondition_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, ColumnSettingCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getColumnSettingCondition_FilterConditions(), theFunctionsPackage.getBasicFunction(), null, "filterConditions", null, 0, 1, ColumnSettingCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(dataSetFilterConditionEClass, DataSetFilterCondition.class, "DataSetFilterCondition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDataSetFilterCondition_FilterConditions(), theFunctionsPackage.getBooleanFunction(), null, "filterConditions", null, 0, 1, DataSetFilterCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDataSetFilterCondition_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, DataSetFilterCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(unionChoiceConditionEClass, UnionChoiceCondition.class, "UnionChoiceCondition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getUnionChoiceCondition_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, UnionChoiceCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getUnionChoiceCondition_ChosenCubes(), theCubesPackage.getFreeBirdToolsCube(), null, "chosenCubes", null, 0, -1, UnionChoiceCondition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(domainMappingModuleEClass, DomainMappingModule.class, "DomainMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDomainMappingModule_LeafModuleToVersionedDTMViewsModules(), this.getLeafModuleToVersionedDTMViewsModule(), null, "leafModuleToVersionedDTMViewsModules", null, 0, -1, DomainMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDomainMappingModule_LeafModuleToVersionedCubeSchemaModules(), this.getLeafModuleToVersionedCubeSchemaModule(), null, "leafModuleToVersionedCubeSchemaModules", null, 0, -1, DomainMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDomainMappingModule_LeafModuleToReportCellViewModules(), this.getLeafModuleToReportCellViewModule(), null, "leafModuleToReportCellViewModules", null, 0, -1, DomainMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDomainMappingModule_ScenarioToCodeConditions(), this.getScenarioToCodeConditions(), null, "scenarioToCodeConditions", null, 0, -1, DomainMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Domain_model_mappingPackageImpl
