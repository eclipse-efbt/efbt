/**
 */
package bird_model.impl;

import bird_model.BIRDModel;
import bird_model.Bird_modelFactory;
import bird_model.Bird_modelPackage;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.openregspecs.model.core.CorePackage;

import org.eclipse.efbt.openregspecs.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.openregspecs.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

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
public class Bird_modelPackageImpl extends EPackageImpl implements Bird_modelPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass birdModelEClass = null;

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
	 * @see bird_model.Bird_modelPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Bird_modelPackageImpl() {
		super(eNS_URI, Bird_modelFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Bird_modelPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Bird_modelPackage init() {
		if (isInited) return (Bird_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Bird_modelPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredBird_modelPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Bird_modelPackageImpl theBird_modelPackage = registeredBird_modelPackage instanceof Bird_modelPackageImpl ? (Bird_modelPackageImpl)registeredBird_modelPackage : new Bird_modelPackageImpl();

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
		theBird_modelPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theopenregspecs_smcubes_core_extensionPackage.createPackageContents();

		// Initialize created meta-data
		theBird_modelPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theopenregspecs_smcubes_core_extensionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theBird_modelPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Bird_modelPackage.eNS_URI, theBird_modelPackage);
		return theBird_modelPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBIRDModel() {
		return birdModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_SmCubesCoreModel() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_EntityModules() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_SQLEntityModules() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_SubProcesses() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_Requirements() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBIRDModel_Tests() {
		return (EReference)birdModelEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bird_modelFactory getBird_modelFactory() {
		return (Bird_modelFactory)getEFactoryInstance();
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
		birdModelEClass = createEClass(BIRD_MODEL);
		createEReference(birdModelEClass, BIRD_MODEL__SM_CUBES_CORE_MODEL);
		createEReference(birdModelEClass, BIRD_MODEL__ENTITY_MODULES);
		createEReference(birdModelEClass, BIRD_MODEL__SQL_ENTITY_MODULES);
		createEReference(birdModelEClass, BIRD_MODEL__SUB_PROCESSES);
		createEReference(birdModelEClass, BIRD_MODEL__REQUIREMENTS);
		createEReference(birdModelEClass, BIRD_MODEL__TESTS);
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
		openregspecs_smcubes_core_extensionPackage theopenregspecs_smcubes_core_extensionPackage = (openregspecs_smcubes_core_extensionPackage)EPackage.Registry.INSTANCE.getEPackage(openregspecs_smcubes_core_extensionPackage.eNS_URI);
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Sql_litePackage theSql_litePackage = (Sql_litePackage)EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);
		Bpmn_litePackage theBpmn_litePackage = (Bpmn_litePackage)EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		TestingPackage theTestingPackage = (TestingPackage)EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(birdModelEClass, BIRDModel.class, "BIRDModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBIRDModel_SmCubesCoreModel(), theopenregspecs_smcubes_core_extensionPackage.getSMCubesCoreModel(), null, "smCubesCoreModel", null, 0, 1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBIRDModel_EntityModules(), theData_meta_modelPackage.getEntityModule(), null, "entityModules", null, 0, -1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBIRDModel_SQLEntityModules(), theSql_litePackage.getSQLEntityModule(), null, "SQLEntityModules", null, 0, -1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBIRDModel_SubProcesses(), theBpmn_litePackage.getSubProcess(), null, "subProcesses", null, 0, -1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBIRDModel_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, -1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBIRDModel_Tests(), theTestingPackage.getTestModule(), null, "tests", null, 0, -1, BIRDModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Bird_modelPackageImpl
