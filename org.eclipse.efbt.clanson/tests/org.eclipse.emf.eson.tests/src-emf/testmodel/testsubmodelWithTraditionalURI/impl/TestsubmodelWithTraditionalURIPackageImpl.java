/**
 */
package testmodel.testsubmodelWithTraditionalURI.impl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import testmodel.TestmodelPackage;

import testmodel.impl.TestmodelPackageImpl;

import testmodel.testsubmodel.TestsubmodelPackage;

import testmodel.testsubmodel.impl.TestsubmodelPackageImpl;

import testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI;
import testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIFactory;
import testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestsubmodelWithTraditionalURIPackageImpl extends EPackageImpl implements TestsubmodelWithTraditionalURIPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testInnerModelInPackageWithTraditionalURIEClass = null;

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
	 * @see testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TestsubmodelWithTraditionalURIPackageImpl() {
		super(eNS_URI, TestsubmodelWithTraditionalURIFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TestsubmodelWithTraditionalURIPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TestsubmodelWithTraditionalURIPackage init() {
		if (isInited) return (TestsubmodelWithTraditionalURIPackage)EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI);

		// Obtain or create and register package
		TestsubmodelWithTraditionalURIPackageImpl theTestsubmodelWithTraditionalURIPackage = (TestsubmodelWithTraditionalURIPackageImpl)(EPackage.Registry.INSTANCE.get(eNS_URI) instanceof TestsubmodelWithTraditionalURIPackageImpl ? EPackage.Registry.INSTANCE.get(eNS_URI) : new TestsubmodelWithTraditionalURIPackageImpl());

		isInited = true;

		// Obtain or create and register interdependencies
		TestmodelPackageImpl theTestmodelPackage = (TestmodelPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestmodelPackage.eNS_URI) instanceof TestmodelPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestmodelPackage.eNS_URI) : TestmodelPackage.eINSTANCE);
		TestsubmodelPackageImpl theTestsubmodelPackage = (TestsubmodelPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI) instanceof TestsubmodelPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI) : TestsubmodelPackage.eINSTANCE);

		// Create package meta-data objects
		theTestsubmodelWithTraditionalURIPackage.createPackageContents();
		theTestmodelPackage.createPackageContents();
		theTestsubmodelPackage.createPackageContents();

		// Initialize created meta-data
		theTestsubmodelWithTraditionalURIPackage.initializePackageContents();
		theTestmodelPackage.initializePackageContents();
		theTestsubmodelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTestsubmodelWithTraditionalURIPackage.freeze();

  
		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TestsubmodelWithTraditionalURIPackage.eNS_URI, theTestsubmodelWithTraditionalURIPackage);
		return theTestsubmodelWithTraditionalURIPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTestInnerModelInPackageWithTraditionalURI() {
		return testInnerModelInPackageWithTraditionalURIEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTestInnerModelInPackageWithTraditionalURI_Name() {
		return (EAttribute)testInnerModelInPackageWithTraditionalURIEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestsubmodelWithTraditionalURIFactory getTestsubmodelWithTraditionalURIFactory() {
		return (TestsubmodelWithTraditionalURIFactory)getEFactoryInstance();
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
		testInnerModelInPackageWithTraditionalURIEClass = createEClass(TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI);
		createEAttribute(testInnerModelInPackageWithTraditionalURIEClass, TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI__NAME);
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

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes and features; add operations and parameters
		initEClass(testInnerModelInPackageWithTraditionalURIEClass, TestInnerModelInPackageWithTraditionalURI.class, "TestInnerModelInPackageWithTraditionalURI", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTestInnerModelInPackageWithTraditionalURI_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestInnerModelInPackageWithTraditionalURI.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
	}

} //TestsubmodelWithTraditionalURIPackageImpl
