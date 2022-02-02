/**
 */
package advanced_row_transformation_logic.impl;

import advanced_row_transformation_logic.Advanced_row_transformation_logicFactory;
import advanced_row_transformation_logic.Advanced_row_transformation_logicPackage;
import advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction;

import attribute_lineage.Attribute_lineagePackage;

import attribute_lineage.impl.Attribute_lineagePackageImpl;

import column_transformation_logic.Column_transformation_logicPackage;

import cube_transformation_logic.Cube_transformation_logicPackage;

import cube_transformation_logic.impl.Cube_transformation_logicPackageImpl;

import entities.EntitiesPackage;

import functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import row_transformation_logic.Row_transformation_logicPackage;

import row_transformation_logic.impl.Row_transformation_logicPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Advanced_row_transformation_logicPackageImpl extends EPackageImpl implements Advanced_row_transformation_logicPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass explodeArrayOfStructsRowFunctionEClass = null;

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
	 * @see advanced_row_transformation_logic.Advanced_row_transformation_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Advanced_row_transformation_logicPackageImpl() {
		super(eNS_URI, Advanced_row_transformation_logicFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Advanced_row_transformation_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Advanced_row_transformation_logicPackage init() {
		if (isInited) return (Advanced_row_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Advanced_row_transformation_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredAdvanced_row_transformation_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Advanced_row_transformation_logicPackageImpl theAdvanced_row_transformation_logicPackage = registeredAdvanced_row_transformation_logicPackage instanceof Advanced_row_transformation_logicPackageImpl ? (Advanced_row_transformation_logicPackageImpl)registeredAdvanced_row_transformation_logicPackage : new Advanced_row_transformation_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		EntitiesPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Attribute_lineagePackage.eNS_URI);
		Attribute_lineagePackageImpl theAttribute_lineagePackage = (Attribute_lineagePackageImpl)(registeredPackage instanceof Attribute_lineagePackageImpl ? registeredPackage : Attribute_lineagePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cube_transformation_logicPackage.eNS_URI);
		Cube_transformation_logicPackageImpl theCube_transformation_logicPackage = (Cube_transformation_logicPackageImpl)(registeredPackage instanceof Cube_transformation_logicPackageImpl ? registeredPackage : Cube_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Row_transformation_logicPackage.eNS_URI);
		Row_transformation_logicPackageImpl theRow_transformation_logicPackage = (Row_transformation_logicPackageImpl)(registeredPackage instanceof Row_transformation_logicPackageImpl ? registeredPackage : Row_transformation_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theAdvanced_row_transformation_logicPackage.createPackageContents();
		theAttribute_lineagePackage.createPackageContents();
		theCube_transformation_logicPackage.createPackageContents();
		theRow_transformation_logicPackage.createPackageContents();

		// Initialize created meta-data
		theAdvanced_row_transformation_logicPackage.initializePackageContents();
		theAttribute_lineagePackage.initializePackageContents();
		theCube_transformation_logicPackage.initializePackageContents();
		theRow_transformation_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theAdvanced_row_transformation_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Advanced_row_transformation_logicPackage.eNS_URI, theAdvanced_row_transformation_logicPackage);
		return theAdvanced_row_transformation_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getExplodeArrayOfStructsRowFunction() {
		return explodeArrayOfStructsRowFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getExplodeArrayOfStructsRowFunction_ArraySourceVariable() {
		return (EReference)explodeArrayOfStructsRowFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_row_transformation_logicFactory getAdvanced_row_transformation_logicFactory() {
		return (Advanced_row_transformation_logicFactory)getEFactoryInstance();
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
		explodeArrayOfStructsRowFunctionEClass = createEClass(EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION);
		createEReference(explodeArrayOfStructsRowFunctionEClass, EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE);
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
		Row_transformation_logicPackage theRow_transformation_logicPackage = (Row_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Row_transformation_logicPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		explodeArrayOfStructsRowFunctionEClass.getESuperTypes().add(theRow_transformation_logicPackage.getRowCreationApproach());

		// Initialize classes, features, and operations; add parameters
		initEClass(explodeArrayOfStructsRowFunctionEClass, ExplodeArrayOfStructsRowFunction.class, "ExplodeArrayOfStructsRowFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExplodeArrayOfStructsRowFunction_ArraySourceVariable(), theEcorePackage.getEStructuralFeature(), null, "arraySourceVariable", null, 0, 1, ExplodeArrayOfStructsRowFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Advanced_row_transformation_logicPackageImpl
