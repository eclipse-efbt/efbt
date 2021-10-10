/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Clause;
import org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText;
import org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid;
import org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint;
import org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Given;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Param;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Then;
import org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.When;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Test_definitionPackageImpl extends EPackageImpl implements Test_definitionPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestContraintsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestTemplateModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestTemplateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestConstraintsModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestDefinitionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass e2EBDDTestDefinitionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unitTestDefinitionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass clauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass paramEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass givenEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass whenEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass thenEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass clauseTextEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass coverageGridEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass coverageGridPointEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestDefinitionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testDefinitionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass regFunctionalityTestDefinitionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass regFunctionalityTestDefinitionModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Test_definitionPackageImpl()
	{
		super(eNS_URI, Test_definitionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Test_definitionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Test_definitionPackage init()
	{
		if (isInited) return (Test_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Test_definitionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTest_definitionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Test_definitionPackageImpl theTest_definitionPackage = registeredTest_definitionPackage instanceof Test_definitionPackageImpl ? (Test_definitionPackageImpl)registeredTest_definitionPackage : new Test_definitionPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		TaskPackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		EcorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Test_input_dataPackage.eNS_URI);
		Test_input_dataPackageImpl theTest_input_dataPackage = (Test_input_dataPackageImpl)(registeredPackage instanceof Test_input_dataPackageImpl ? registeredPackage : Test_input_dataPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TestPackage.eNS_URI);
		TestPackageImpl theTestPackage = (TestPackageImpl)(registeredPackage instanceof TestPackageImpl ? registeredPackage : TestPackage.eINSTANCE);

		// Create package meta-data objects
		theTest_definitionPackage.createPackageContents();
		theTest_input_dataPackage.createPackageContents();
		theTestPackage.createPackageContents();

		// Initialize created meta-data
		theTest_definitionPackage.initializePackageContents();
		theTest_input_dataPackage.initializePackageContents();
		theTestPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTest_definitionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Test_definitionPackage.eNS_URI, theTest_definitionPackage);
		return theTest_definitionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestContraints()
	{
		return bddTestContraintsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestContraints_Template()
	{
		return (EReference)bddTestContraintsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getBDDTestContraints_Name()
	{
		return (EAttribute)bddTestContraintsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestContraints_GivenParams()
	{
		return (EReference)bddTestContraintsEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestContraints_WhenParams()
	{
		return (EReference)bddTestContraintsEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestContraints_ThenParams()
	{
		return (EReference)bddTestContraintsEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestTemplateModule()
	{
		return bddTestTemplateModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplateModule_Templates()
	{
		return (EReference)bddTestTemplateModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestTemplate()
	{
		return bddTestTemplateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getBDDTestTemplate_Name()
	{
		return (EAttribute)bddTestTemplateEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_GivenText()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_WhenText()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_ThenText()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_GivenParams()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_WhenParams()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestTemplate_ThenParams()
	{
		return (EReference)bddTestTemplateEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestConstraintsModule()
	{
		return bddTestConstraintsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestConstraintsModule_CoverageTestSets()
	{
		return (EReference)bddTestConstraintsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestDefinition()
	{
		return bddTestDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestDefinition_TestContraints()
	{
		return (EReference)bddTestDefinitionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestDefinition_Given()
	{
		return (EReference)bddTestDefinitionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestDefinition_When()
	{
		return (EReference)bddTestDefinitionEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestDefinition_Then()
	{
		return (EReference)bddTestDefinitionEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getE2EBDDTestDefinition()
	{
		return e2EBDDTestDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUnitTestDefinition()
	{
		return unitTestDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getClause()
	{
		return clauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getClause_Text()
	{
		return (EReference)clauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getClause_Params()
	{
		return (EReference)clauseEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getClause_RegulatoryTextLinks()
	{
		return (EReference)clauseEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getParam()
	{
		return paramEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getParam_Param()
	{
		return (EReference)paramEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGiven()
	{
		return givenEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getWhen()
	{
		return whenEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getThen()
	{
		return thenEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getClauseText()
	{
		return clauseTextEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getClauseText_Name()
	{
		return (EAttribute)clauseTextEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCoverageGrid()
	{
		return coverageGridEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCoverageGrid_CoverageTestSet()
	{
		return (EReference)coverageGridEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCoverageGrid_CoverageGridPoints()
	{
		return (EReference)coverageGridEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCoverageGrid_Name()
	{
		return (EAttribute)coverageGridEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCoverageGridPoint()
	{
		return coverageGridPointEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCoverageGridPoint_Count()
	{
		return (EAttribute)coverageGridPointEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCoverageGridPoint_Objects()
	{
		return (EReference)coverageGridPointEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTestDefinitionModule()
	{
		return bddTestDefinitionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTestDefinitionModule_TestDefinitions()
	{
		return (EReference)bddTestDefinitionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestDefinition()
	{
		return testDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTestDefinition_Name()
	{
		return (EAttribute)testDefinitionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRegFunctionalityTestDefinition()
	{
		return regFunctionalityTestDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinition_InputModules()
	{
		return (EReference)regFunctionalityTestDefinitionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinition_ProcessingModules()
	{
		return (EReference)regFunctionalityTestDefinitionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinition_ReportCellCreationModules()
	{
		return (EReference)regFunctionalityTestDefinitionEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinition_Scenarios()
	{
		return (EReference)regFunctionalityTestDefinitionEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinition_Picture() {
		return (EReference)regFunctionalityTestDefinitionEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRegFunctionalityTestDefinitionModule()
	{
		return regFunctionalityTestDefinitionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTestDefinitionModule_TestDefinitions()
	{
		return (EReference)regFunctionalityTestDefinitionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test_definitionFactory getTest_definitionFactory()
	{
		return (Test_definitionFactory)getEFactoryInstance();
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
		bddTestContraintsEClass = createEClass(BDD_TEST_CONTRAINTS);
		createEReference(bddTestContraintsEClass, BDD_TEST_CONTRAINTS__TEMPLATE);
		createEAttribute(bddTestContraintsEClass, BDD_TEST_CONTRAINTS__NAME);
		createEReference(bddTestContraintsEClass, BDD_TEST_CONTRAINTS__GIVEN_PARAMS);
		createEReference(bddTestContraintsEClass, BDD_TEST_CONTRAINTS__WHEN_PARAMS);
		createEReference(bddTestContraintsEClass, BDD_TEST_CONTRAINTS__THEN_PARAMS);

		bddTestTemplateModuleEClass = createEClass(BDD_TEST_TEMPLATE_MODULE);
		createEReference(bddTestTemplateModuleEClass, BDD_TEST_TEMPLATE_MODULE__TEMPLATES);

		bddTestTemplateEClass = createEClass(BDD_TEST_TEMPLATE);
		createEAttribute(bddTestTemplateEClass, BDD_TEST_TEMPLATE__NAME);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__GIVEN_TEXT);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__WHEN_TEXT);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__THEN_TEXT);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__GIVEN_PARAMS);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__WHEN_PARAMS);
		createEReference(bddTestTemplateEClass, BDD_TEST_TEMPLATE__THEN_PARAMS);

		bddTestConstraintsModuleEClass = createEClass(BDD_TEST_CONSTRAINTS_MODULE);
		createEReference(bddTestConstraintsModuleEClass, BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS);

		bddTestDefinitionEClass = createEClass(BDD_TEST_DEFINITION);
		createEReference(bddTestDefinitionEClass, BDD_TEST_DEFINITION__TEST_CONTRAINTS);
		createEReference(bddTestDefinitionEClass, BDD_TEST_DEFINITION__GIVEN);
		createEReference(bddTestDefinitionEClass, BDD_TEST_DEFINITION__WHEN);
		createEReference(bddTestDefinitionEClass, BDD_TEST_DEFINITION__THEN);

		e2EBDDTestDefinitionEClass = createEClass(E2EBDD_TEST_DEFINITION);

		unitTestDefinitionEClass = createEClass(UNIT_TEST_DEFINITION);

		clauseEClass = createEClass(CLAUSE);
		createEReference(clauseEClass, CLAUSE__TEXT);
		createEReference(clauseEClass, CLAUSE__PARAMS);
		createEReference(clauseEClass, CLAUSE__REGULATORY_TEXT_LINKS);

		paramEClass = createEClass(PARAM);
		createEReference(paramEClass, PARAM__PARAM);

		givenEClass = createEClass(GIVEN);

		whenEClass = createEClass(WHEN);

		thenEClass = createEClass(THEN);

		clauseTextEClass = createEClass(CLAUSE_TEXT);
		createEAttribute(clauseTextEClass, CLAUSE_TEXT__NAME);

		coverageGridEClass = createEClass(COVERAGE_GRID);
		createEReference(coverageGridEClass, COVERAGE_GRID__COVERAGE_TEST_SET);
		createEReference(coverageGridEClass, COVERAGE_GRID__COVERAGE_GRID_POINTS);
		createEAttribute(coverageGridEClass, COVERAGE_GRID__NAME);

		coverageGridPointEClass = createEClass(COVERAGE_GRID_POINT);
		createEAttribute(coverageGridPointEClass, COVERAGE_GRID_POINT__COUNT);
		createEReference(coverageGridPointEClass, COVERAGE_GRID_POINT__OBJECTS);

		bddTestDefinitionModuleEClass = createEClass(BDD_TEST_DEFINITION_MODULE);
		createEReference(bddTestDefinitionModuleEClass, BDD_TEST_DEFINITION_MODULE__TEST_DEFINITIONS);

		testDefinitionEClass = createEClass(TEST_DEFINITION);
		createEAttribute(testDefinitionEClass, TEST_DEFINITION__NAME);

		regFunctionalityTestDefinitionEClass = createEClass(REG_FUNCTIONALITY_TEST_DEFINITION);
		createEReference(regFunctionalityTestDefinitionEClass, REG_FUNCTIONALITY_TEST_DEFINITION__INPUT_MODULES);
		createEReference(regFunctionalityTestDefinitionEClass, REG_FUNCTIONALITY_TEST_DEFINITION__PROCESSING_MODULES);
		createEReference(regFunctionalityTestDefinitionEClass, REG_FUNCTIONALITY_TEST_DEFINITION__REPORT_CELL_CREATION_MODULES);
		createEReference(regFunctionalityTestDefinitionEClass, REG_FUNCTIONALITY_TEST_DEFINITION__SCENARIOS);
		createEReference(regFunctionalityTestDefinitionEClass, REG_FUNCTIONALITY_TEST_DEFINITION__PICTURE);

		regFunctionalityTestDefinitionModuleEClass = createEClass(REG_FUNCTIONALITY_TEST_DEFINITION_MODULE);
		createEReference(regFunctionalityTestDefinitionModuleEClass, REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS);
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
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		TaskPackage theTaskPackage = (TaskPackage)EPackage.Registry.INSTANCE.getEPackage(TaskPackage.eNS_URI);
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		bddTestTemplateModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		bddTestConstraintsModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		bddTestDefinitionEClass.getESuperTypes().add(this.getTestDefinition());
		e2EBDDTestDefinitionEClass.getESuperTypes().add(this.getBDDTestDefinition());
		unitTestDefinitionEClass.getESuperTypes().add(this.getBDDTestDefinition());
		givenEClass.getESuperTypes().add(this.getClause());
		whenEClass.getESuperTypes().add(this.getClause());
		thenEClass.getESuperTypes().add(this.getClause());
		bddTestDefinitionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		regFunctionalityTestDefinitionEClass.getESuperTypes().add(this.getTestDefinition());
		regFunctionalityTestDefinitionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(bddTestContraintsEClass, BDDTestContraints.class, "BDDTestContraints", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTestContraints_Template(), this.getBDDTestTemplate(), null, "template", null, 0, 1, BDDTestContraints.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getBDDTestContraints_Name(), ecorePackage.getEString(), "name", null, 0, 1, BDDTestContraints.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestContraints_GivenParams(), this.getParam(), null, "givenParams", null, 0, -1, BDDTestContraints.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestContraints_WhenParams(), this.getParam(), null, "whenParams", null, 0, -1, BDDTestContraints.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestContraints_ThenParams(), this.getParam(), null, "thenParams", null, 0, -1, BDDTestContraints.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(bddTestTemplateModuleEClass, BDDTestTemplateModule.class, "BDDTestTemplateModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTestTemplateModule_Templates(), this.getBDDTestTemplate(), null, "templates", null, 0, -1, BDDTestTemplateModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(bddTestTemplateEClass, BDDTestTemplate.class, "BDDTestTemplate", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBDDTestTemplate_Name(), ecorePackage.getEString(), "name", null, 0, 1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_GivenText(), this.getClauseText(), null, "givenText", null, 0, 1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_WhenText(), this.getClauseText(), null, "whenText", null, 0, 1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_ThenText(), this.getClauseText(), null, "thenText", null, 0, 1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_GivenParams(), this.getParam(), null, "givenParams", null, 0, -1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_WhenParams(), this.getParam(), null, "whenParams", null, 0, -1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestTemplate_ThenParams(), this.getParam(), null, "thenParams", null, 0, -1, BDDTestTemplate.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(bddTestConstraintsModuleEClass, BDDTestConstraintsModule.class, "BDDTestConstraintsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTestConstraintsModule_CoverageTestSets(), this.getBDDTestContraints(), null, "coverageTestSets", null, 0, -1, BDDTestConstraintsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(bddTestDefinitionEClass, BDDTestDefinition.class, "BDDTestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTestDefinition_TestContraints(), this.getBDDTestContraints(), null, "testContraints", null, 0, 1, BDDTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestDefinition_Given(), this.getGiven(), null, "given", null, 0, 1, BDDTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestDefinition_When(), this.getWhen(), null, "when", null, 0, 1, BDDTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBDDTestDefinition_Then(), this.getThen(), null, "then", null, 0, 1, BDDTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(e2EBDDTestDefinitionEClass, E2EBDDTestDefinition.class, "E2EBDDTestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(unitTestDefinitionEClass, UnitTestDefinition.class, "UnitTestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(clauseEClass, Clause.class, "Clause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getClause_Text(), this.getClauseText(), null, "text", null, 0, 1, Clause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getClause_Params(), this.getParam(), null, "params", null, 0, -1, Clause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getClause_RegulatoryTextLinks(), theRequirements_textPackage.getTitledRequirementsSection(), null, "regulatoryTextLinks", null, 0, -1, Clause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(paramEClass, Param.class, "Param", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getParam_Param(), ecorePackage.getEObject(), null, "param", null, 0, 1, Param.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(givenEClass, Given.class, "Given", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(whenEClass, When.class, "When", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(thenEClass, Then.class, "Then", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(clauseTextEClass, ClauseText.class, "ClauseText", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getClauseText_Name(), ecorePackage.getEString(), "name", null, 0, 1, ClauseText.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(coverageGridEClass, CoverageGrid.class, "CoverageGrid", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCoverageGrid_CoverageTestSet(), this.getBDDTestContraints(), null, "coverageTestSet", null, 0, 1, CoverageGrid.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCoverageGrid_CoverageGridPoints(), this.getCoverageGridPoint(), null, "coverageGridPoints", null, 0, 1, CoverageGrid.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCoverageGrid_Name(), ecorePackage.getEString(), "name", null, 0, 1, CoverageGrid.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(coverageGridPointEClass, CoverageGridPoint.class, "CoverageGridPoint", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCoverageGridPoint_Count(), ecorePackage.getEInt(), "count", null, 0, 1, CoverageGridPoint.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCoverageGridPoint_Objects(), ecorePackage.getEObject(), null, "objects", null, 0, -1, CoverageGridPoint.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(bddTestDefinitionModuleEClass, BDDTestDefinitionModule.class, "BDDTestDefinitionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTestDefinitionModule_TestDefinitions(), this.getBDDTestDefinition(), null, "testDefinitions", null, 0, -1, BDDTestDefinitionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testDefinitionEClass, TestDefinition.class, "TestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTestDefinition_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(regFunctionalityTestDefinitionEClass, RegFunctionalityTestDefinition.class, "RegFunctionalityTestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRegFunctionalityTestDefinition_InputModules(), theTaskPackage.getReadDataSourceTask(), null, "inputModules", null, 0, -1, RegFunctionalityTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRegFunctionalityTestDefinition_ProcessingModules(), theTaskPackage.getTask(), null, "processingModules", null, 0, -1, RegFunctionalityTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRegFunctionalityTestDefinition_ReportCellCreationModules(), theTaskPackage.getReportCellCreationTask(), null, "reportCellCreationModules", null, 0, -1, RegFunctionalityTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRegFunctionalityTestDefinition_Scenarios(), theScenariosPackage.getScenario(), null, "scenarios", null, 0, -1, RegFunctionalityTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRegFunctionalityTestDefinition_Picture(), theTaskPackage.getTaskSet(), null, "picture", null, 0, 1, RegFunctionalityTestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(regFunctionalityTestDefinitionModuleEClass, RegFunctionalityTestDefinitionModule.class, "RegFunctionalityTestDefinitionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRegFunctionalityTestDefinitionModule_TestDefinitions(), this.getRegFunctionalityTestDefinition(), null, "testDefinitions", null, 0, -1, RegFunctionalityTestDefinitionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations()
	{
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

} //Test_definitionPackageImpl
