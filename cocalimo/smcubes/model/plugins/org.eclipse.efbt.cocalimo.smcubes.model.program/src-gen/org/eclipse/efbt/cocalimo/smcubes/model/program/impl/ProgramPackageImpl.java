/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.impl;

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

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData;
import org.eclipse.efbt.cocalimo.smcubes.model.program.StaticModel;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

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
public class ProgramPackageImpl extends EPackageImpl implements ProgramPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesStaticModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesTestInputDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesReportResultsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesBDDTestEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesBDDTestModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass staticModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesRegFunctionalityTestEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesRegFunctionalityTestModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private ProgramPackageImpl() {
		super(eNS_URI, ProgramFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link ProgramPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static ProgramPackage init() {
		if (isInited) return (ProgramPackage)EPackage.Registry.INSTANCE.getEPackage(ProgramPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredProgramPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		ProgramPackageImpl theProgramPackage = registeredProgramPackage instanceof ProgramPackageImpl ? (ProgramPackageImpl)registeredProgramPackage : new ProgramPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
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

		// Create package meta-data objects
		theProgramPackage.createPackageContents();

		// Initialize created meta-data
		theProgramPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theProgramPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(ProgramPackage.eNS_URI, theProgramPackage);
		return theProgramPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesStaticModel() {
		return smCubesStaticModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_SmCubesModel() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_Requirements() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_BddTestDefinitions() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_Tests() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_TestTemplates() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_TestConstriants() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_FunctionalityModules() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_RegFunctionalityTests() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesStaticModel_RegFunctionalityTestDefinitions() {
		return (EReference)smCubesStaticModelEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesTestInputData() {
		return smCubesTestInputDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesTestInputData_Smcubes_inputdata() {
		return (EReference)smCubesTestInputDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesReportResults() {
		return smCubesReportResultsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesReportResults_ReportCells() {
		return (EReference)smCubesReportResultsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesBDDTest() {
		return smCubesBDDTestEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesBDDTest_InputData() {
		return (EReference)smCubesBDDTestEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesBDDTest_ExpectedResults() {
		return (EReference)smCubesBDDTestEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesBDDTestModule() {
		return smCubesBDDTestModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesBDDTestModule_Tests() {
		return (EReference)smCubesBDDTestModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getStaticModel() {
		return staticModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getStaticModel_Name() {
		return (EAttribute)staticModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesRegFunctionalityTest() {
		return smCubesRegFunctionalityTestEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesRegFunctionalityTest_InputData() {
		return (EReference)smCubesRegFunctionalityTestEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesRegFunctionalityTest_ExpectedResults() {
		return (EReference)smCubesRegFunctionalityTestEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesRegFunctionalityTestModule() {
		return smCubesRegFunctionalityTestModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSMCubesRegFunctionalityTestModule_RegFunctionalityTests() {
		return (EReference)smCubesRegFunctionalityTestModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ProgramFactory getProgramFactory() {
		return (ProgramFactory)getEFactoryInstance();
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
		smCubesStaticModelEClass = createEClass(SM_CUBES_STATIC_MODEL);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__REQUIREMENTS);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__TESTS);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__TEST_TEMPLATES);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS);
		createEReference(smCubesStaticModelEClass, SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS);

		smCubesTestInputDataEClass = createEClass(SM_CUBES_TEST_INPUT_DATA);
		createEReference(smCubesTestInputDataEClass, SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA);

		smCubesReportResultsEClass = createEClass(SM_CUBES_REPORT_RESULTS);
		createEReference(smCubesReportResultsEClass, SM_CUBES_REPORT_RESULTS__REPORT_CELLS);

		smCubesBDDTestEClass = createEClass(SM_CUBES_BDD_TEST);
		createEReference(smCubesBDDTestEClass, SM_CUBES_BDD_TEST__INPUT_DATA);
		createEReference(smCubesBDDTestEClass, SM_CUBES_BDD_TEST__EXPECTED_RESULTS);

		smCubesBDDTestModuleEClass = createEClass(SM_CUBES_BDD_TEST_MODULE);
		createEReference(smCubesBDDTestModuleEClass, SM_CUBES_BDD_TEST_MODULE__TESTS);

		staticModelEClass = createEClass(STATIC_MODEL);
		createEAttribute(staticModelEClass, STATIC_MODEL__NAME);

		smCubesRegFunctionalityTestEClass = createEClass(SM_CUBES_REG_FUNCTIONALITY_TEST);
		createEReference(smCubesRegFunctionalityTestEClass, SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA);
		createEReference(smCubesRegFunctionalityTestEClass, SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS);

		smCubesRegFunctionalityTestModuleEClass = createEClass(SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE);
		createEReference(smCubesRegFunctionalityTestModuleEClass, SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS);
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
		Smcubes_modelPackage theSmcubes_modelPackage = (Smcubes_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Smcubes_modelPackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Test_definitionPackage theTest_definitionPackage = (Test_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Test_definitionPackage.eNS_URI);
		Functionality_modulePackage theFunctionality_modulePackage = (Functionality_modulePackage)EPackage.Registry.INSTANCE.getEPackage(Functionality_modulePackage.eNS_URI);
		Test_input_dataPackage theTest_input_dataPackage = (Test_input_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Test_input_dataPackage.eNS_URI);
		Base_column_structured_dataPackage theBase_column_structured_dataPackage = (Base_column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Base_column_structured_dataPackage.eNS_URI);
		TestPackage theTestPackage = (TestPackage)EPackage.Registry.INSTANCE.getEPackage(TestPackage.eNS_URI);
		ReportsPackage theReportsPackage = (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		smCubesStaticModelEClass.getESuperTypes().add(this.getStaticModel());
		smCubesTestInputDataEClass.getESuperTypes().add(theTest_input_dataPackage.getTestInputData());
		smCubesReportResultsEClass.getESuperTypes().add(theTestPackage.getResult());
		smCubesBDDTestEClass.getESuperTypes().add(theTestPackage.getBDDTest());
		smCubesBDDTestModuleEClass.getESuperTypes().add(theTestPackage.getTestModule());
		smCubesRegFunctionalityTestEClass.getESuperTypes().add(theTestPackage.getRegFunctionalityTest());
		smCubesRegFunctionalityTestModuleEClass.getESuperTypes().add(theTestPackage.getTestModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(smCubesStaticModelEClass, SMCubesStaticModel.class, "SMCubesStaticModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesStaticModel_SmCubesModel(), theSmcubes_modelPackage.getSmcubesModel(), null, "smCubesModel", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_BddTestDefinitions(), theTest_definitionPackage.getBDDTestDefinitionModule(), null, "bddTestDefinitions", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_Tests(), this.getSMCubesBDDTestModule(), null, "tests", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_TestTemplates(), theTest_definitionPackage.getBDDTestTemplateModule(), null, "testTemplates", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_TestConstriants(), theTest_definitionPackage.getBDDTestContraints(), null, "testConstriants", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_FunctionalityModules(), theFunctionality_modulePackage.getFunctionalityModuleModule(), null, "functionalityModules", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_RegFunctionalityTests(), this.getSMCubesRegFunctionalityTestModule(), null, "regFunctionalityTests", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesStaticModel_RegFunctionalityTestDefinitions(), theTest_definitionPackage.getRegFunctionalityTestDefinitionModule(), null, "regFunctionalityTestDefinitions", null, 0, 1, SMCubesStaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesTestInputDataEClass, SMCubesTestInputData.class, "SMCubesTestInputData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesTestInputData_Smcubes_inputdata(), theBase_column_structured_dataPackage.getBaseColumnStructuredData(), null, "smcubes_inputdata", null, 0, -1, SMCubesTestInputData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesReportResultsEClass, SMCubesReportResults.class, "SMCubesReportResults", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesReportResults_ReportCells(), theReportsPackage.getReportCellValue(), null, "reportCells", null, 0, -1, SMCubesReportResults.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesBDDTestEClass, SMCubesBDDTest.class, "SMCubesBDDTest", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesBDDTest_InputData(), this.getSMCubesTestInputData(), null, "inputData", null, 0, 1, SMCubesBDDTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesBDDTest_ExpectedResults(), this.getSMCubesReportResults(), null, "expectedResults", null, 0, 1, SMCubesBDDTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesBDDTestModuleEClass, SMCubesBDDTestModule.class, "SMCubesBDDTestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesBDDTestModule_Tests(), this.getSMCubesBDDTest(), null, "tests", null, 0, -1, SMCubesBDDTestModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(staticModelEClass, StaticModel.class, "StaticModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getStaticModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, StaticModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesRegFunctionalityTestEClass, SMCubesRegFunctionalityTest.class, "SMCubesRegFunctionalityTest", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesRegFunctionalityTest_InputData(), this.getSMCubesTestInputData(), null, "inputData", null, 0, 1, SMCubesRegFunctionalityTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSMCubesRegFunctionalityTest_ExpectedResults(), this.getSMCubesReportResults(), null, "expectedResults", null, 0, 1, SMCubesRegFunctionalityTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesRegFunctionalityTestModuleEClass, SMCubesRegFunctionalityTestModule.class, "SMCubesRegFunctionalityTestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSMCubesRegFunctionalityTestModule_RegFunctionalityTests(), this.getSMCubesRegFunctionalityTest(), null, "regFunctionalityTests", null, 0, -1, SMCubesRegFunctionalityTestModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
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

} //ProgramPackageImpl
