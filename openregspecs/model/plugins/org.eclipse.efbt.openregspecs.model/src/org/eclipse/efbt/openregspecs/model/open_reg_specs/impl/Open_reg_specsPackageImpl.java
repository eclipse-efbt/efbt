/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs.impl;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.openregspecs.model.core.CorePackage;

import org.eclipse.efbt.openregspecs.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.openregspecs.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsFactory;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

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
public class Open_reg_specsPackageImpl extends EPackageImpl implements Open_reg_specsPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass openRegSpecsEClass = null;

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
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Open_reg_specsPackageImpl() {
		super(eNS_URI, Open_reg_specsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Open_reg_specsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Open_reg_specsPackage init() {
		if (isInited) return (Open_reg_specsPackage)EPackage.Registry.INSTANCE.getEPackage(Open_reg_specsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredOpen_reg_specsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Open_reg_specsPackageImpl theOpen_reg_specsPackage = registeredOpen_reg_specsPackage instanceof Open_reg_specsPackageImpl ? (Open_reg_specsPackageImpl)registeredOpen_reg_specsPackage : new Open_reg_specsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Data_meta_modelPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Sql_litePackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		TestingPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(openregspecs_smcubes_core_extensionPackage.eNS_URI);
		openregspecs_smcubes_core_extensionPackageImpl theopenregspecs_smcubes_core_extensionPackage = (openregspecs_smcubes_core_extensionPackageImpl)(registeredPackage instanceof openregspecs_smcubes_core_extensionPackageImpl ? registeredPackage : openregspecs_smcubes_core_extensionPackage.eINSTANCE);

		// Create package meta-data objects
		theOpen_reg_specsPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theopenregspecs_smcubes_core_extensionPackage.createPackageContents();

		// Initialize created meta-data
		theOpen_reg_specsPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theopenregspecs_smcubes_core_extensionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theOpen_reg_specsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Open_reg_specsPackage.eNS_URI, theOpen_reg_specsPackage);
		return theOpen_reg_specsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOpenRegSpecs() {
		return openRegSpecsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Requirements() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Types_and_concepts() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Data_model() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Process_workflow() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Report_generation() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOpenRegSpecs_Tests() {
		return (EReference)openRegSpecsEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Open_reg_specsFactory getOpen_reg_specsFactory() {
		return (Open_reg_specsFactory)getEFactoryInstance();
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
		openRegSpecsEClass = createEClass(OPEN_REG_SPECS);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__REQUIREMENTS);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__TYPES_AND_CONCEPTS);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__DATA_MODEL);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__PROCESS_WORKFLOW);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__REPORT_GENERATION);
		createEReference(openRegSpecsEClass, OPEN_REG_SPECS__TESTS);
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
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		openregspecs_smcubes_core_extensionPackage theopenregspecs_smcubes_core_extensionPackage = (openregspecs_smcubes_core_extensionPackage)EPackage.Registry.INSTANCE.getEPackage(openregspecs_smcubes_core_extensionPackage.eNS_URI);
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Bpmn_litePackage theBpmn_litePackage = (Bpmn_litePackage)EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Sql_litePackage theSql_litePackage = (Sql_litePackage)EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);
		TestingPackage theTestingPackage = (TestingPackage)EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(openRegSpecsEClass, OpenRegSpecs.class, "OpenRegSpecs", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getOpenRegSpecs_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, -1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getOpenRegSpecs_Types_and_concepts(), theopenregspecs_smcubes_core_extensionPackage.getSMCubesCoreModel(), null, "types_and_concepts", null, 0, 1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getOpenRegSpecs_Data_model(), theData_meta_modelPackage.getEntityModule(), null, "data_model", null, 0, -1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getOpenRegSpecs_Process_workflow(), theBpmn_litePackage.getSubProcess(), null, "process_workflow", null, 0, -1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getOpenRegSpecs_Report_generation(), theSql_litePackage.getSQLEntityModule(), null, "report_generation", null, 0, -1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getOpenRegSpecs_Tests(), theTestingPackage.getTestModule(), null, "tests", null, 0, -1, OpenRegSpecs.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Open_reg_specsPackageImpl
