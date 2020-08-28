/**
 */
package testmodel.testsubmodel.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import testmodel.TestmodelPackage;

import testmodel.impl.TestmodelPackageImpl;

import testmodel.testsubmodel.TestInnerModel;
import testmodel.testsubmodel.TestsubmodelFactory;
import testmodel.testsubmodel.TestsubmodelPackage;
import testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIPackage;
import testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestsubmodelPackageImpl extends EPackageImpl implements TestsubmodelPackage {
    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    private EClass testInnerModelEClass = null;

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
	 * @see testmodel.testsubmodel.TestsubmodelPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
    private TestsubmodelPackageImpl() {
		super(eNS_URI, TestsubmodelFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TestsubmodelPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
    public static TestsubmodelPackage init() {
		if (isInited) return (TestsubmodelPackage)EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI);

		// Obtain or create and register package
		TestsubmodelPackageImpl theTestsubmodelPackage = (TestsubmodelPackageImpl)(EPackage.Registry.INSTANCE.get(eNS_URI) instanceof TestsubmodelPackageImpl ? EPackage.Registry.INSTANCE.get(eNS_URI) : new TestsubmodelPackageImpl());

		isInited = true;

		// Obtain or create and register interdependencies
		TestmodelPackageImpl theTestmodelPackage = (TestmodelPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestmodelPackage.eNS_URI) instanceof TestmodelPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestmodelPackage.eNS_URI) : TestmodelPackage.eINSTANCE);
		TestsubmodelWithTraditionalURIPackageImpl theTestsubmodelWithTraditionalURIPackage = (TestsubmodelWithTraditionalURIPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI) instanceof TestsubmodelWithTraditionalURIPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI) : TestsubmodelWithTraditionalURIPackage.eINSTANCE);

		// Create package meta-data objects
		theTestsubmodelPackage.createPackageContents();
		theTestmodelPackage.createPackageContents();
		theTestsubmodelWithTraditionalURIPackage.createPackageContents();

		// Initialize created meta-data
		theTestsubmodelPackage.initializePackageContents();
		theTestmodelPackage.initializePackageContents();
		theTestsubmodelWithTraditionalURIPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTestsubmodelPackage.freeze();

  
		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TestsubmodelPackage.eNS_URI, theTestsubmodelPackage);
		return theTestsubmodelPackage;
	}

    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public EClass getTestInnerModel() {
		return testInnerModelEClass;
	}

    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public TestsubmodelFactory getTestsubmodelFactory() {
		return (TestsubmodelFactory)getEFactoryInstance();
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
		testInnerModelEClass = createEClass(TEST_INNER_MODEL);
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
		initEClass(testInnerModelEClass, TestInnerModel.class, "TestInnerModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
	}

} //TestsubmodelPackageImpl
