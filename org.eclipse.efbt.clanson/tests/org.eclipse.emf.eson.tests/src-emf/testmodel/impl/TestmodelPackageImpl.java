/**
 */
package testmodel.impl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.impl.EPackageImpl;

import testmodel.Abstract;
import testmodel.AttributeSample;
import testmodel.AttributeTestContainer;
import testmodel.Child;
import testmodel.CustomName;
import testmodel.DefaultName;
import testmodel.Interface;
import testmodel.NameAttributeContainer;
import testmodel.NestedElement;
import testmodel.NestedElements;
import testmodel.NoName;
import testmodel.ReferenceTarget;
import testmodel.ReferenceTargetSubclass;
import testmodel.ReferenceTestContainer;
import testmodel.SampleEnum;
import testmodel.SingleOptional;
import testmodel.SingleRequired;
import testmodel.TestModel;
import testmodel.TestmodelFactory;
import testmodel.TestmodelPackage;
import testmodel.testsubmodel.TestsubmodelPackage;
import testmodel.testsubmodel.impl.TestsubmodelPackageImpl;
import testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIPackage;
import testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestmodelPackageImpl extends EPackageImpl implements TestmodelPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass childEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass singleOptionalEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass singleRequiredEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass interfaceEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass abstractEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass nestedElementsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass nestedElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeSampleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass referenceTestContainerEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass referenceTargetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass referenceTargetSubclassEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeTestContainerEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass nameAttributeContainerEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass defaultNameEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass customNameEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass noNameEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum sampleEnumEEnum = null;

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
	 * @see testmodel.TestmodelPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TestmodelPackageImpl() {
		super(eNS_URI, TestmodelFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TestmodelPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TestmodelPackage init() {
		if (isInited) return (TestmodelPackage)EPackage.Registry.INSTANCE.getEPackage(TestmodelPackage.eNS_URI);

		// Obtain or create and register package
		TestmodelPackageImpl theTestmodelPackage = (TestmodelPackageImpl)(EPackage.Registry.INSTANCE.get(eNS_URI) instanceof TestmodelPackageImpl ? EPackage.Registry.INSTANCE.get(eNS_URI) : new TestmodelPackageImpl());

		isInited = true;

		// Obtain or create and register interdependencies
		TestsubmodelPackageImpl theTestsubmodelPackage = (TestsubmodelPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI) instanceof TestsubmodelPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI) : TestsubmodelPackage.eINSTANCE);
		TestsubmodelWithTraditionalURIPackageImpl theTestsubmodelWithTraditionalURIPackage = (TestsubmodelWithTraditionalURIPackageImpl)(EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI) instanceof TestsubmodelWithTraditionalURIPackageImpl ? EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI) : TestsubmodelWithTraditionalURIPackage.eINSTANCE);

		// Create package meta-data objects
		theTestmodelPackage.createPackageContents();
		theTestsubmodelPackage.createPackageContents();
		theTestsubmodelWithTraditionalURIPackage.createPackageContents();

		// Initialize created meta-data
		theTestmodelPackage.initializePackageContents();
		theTestsubmodelPackage.initializePackageContents();
		theTestsubmodelWithTraditionalURIPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTestmodelPackage.freeze();

  
		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TestmodelPackage.eNS_URI, theTestmodelPackage);
		return theTestmodelPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTestModel() {
		return testModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_Children() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_SingleOptional() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_SingleRequired() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTestModel_Name() {
		return (EAttribute)testModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_AttributeSample() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_LinkToSingleRequired() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_ReferenceTestContainer() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_AttributeTest() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_NameAttributeTest() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModel_Sub() {
		return (EReference)testModelEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getChild() {
		return childEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getChild_ReferenceList() {
		return (EReference)childEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSingleOptional() {
		return singleOptionalEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSingleRequired() {
		return singleRequiredEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSingleRequired_NestedElements() {
		return (EReference)singleRequiredEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSingleRequired_NestedElement() {
		return (EReference)singleRequiredEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getSingleRequired_Name() {
		return (EAttribute)singleRequiredEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getInterface() {
		return interfaceEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAbstract() {
		return abstractEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAbstract_ParentAttribute() {
		return (EAttribute)abstractEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAbstract_ParentReference() {
		return (EReference)abstractEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAbstract_ParentContainment() {
		return (EReference)abstractEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getNestedElements() {
		return nestedElementsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getNestedElement() {
		return nestedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getNestedElement_Id() {
		return (EAttribute)nestedElementEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeSample() {
		return attributeSampleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeSample_SingleIntOptional() {
		return (EAttribute)attributeSampleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeSample_ListIntRequired() {
		return (EAttribute)attributeSampleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeSample_BigDecimal() {
		return (EAttribute)attributeSampleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeSample_BigInteger() {
		return (EAttribute)attributeSampleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeSample_Double() {
		return (EAttribute)attributeSampleEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getReferenceTestContainer() {
		return referenceTestContainerEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getReferenceTestContainer_Containments() {
		return (EReference)referenceTestContainerEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getReferenceTestContainer_ReferenceToOne() {
		return (EReference)referenceTestContainerEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getReferenceTestContainer_ReferenceToMany() {
		return (EReference)referenceTestContainerEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getReferenceTestContainer_Name() {
		return (EAttribute)referenceTestContainerEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getReferenceTestContainer_Containment() {
		return (EReference)referenceTestContainerEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getReferenceTestContainer_OneAttribute() {
		return (EAttribute)referenceTestContainerEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getReferenceTestContainer_ManyAttributes() {
		return (EAttribute)referenceTestContainerEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getReferenceTarget() {
		return referenceTargetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getReferenceTarget_Name() {
		return (EAttribute)referenceTargetEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getReferenceTargetSubclass() {
		return referenceTargetSubclassEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeTestContainer() {
		return attributeTestContainerEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_Name() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneInt() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneInteger() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyInt() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneBool() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyBool() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneDouble() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyDouble() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneBigDecimal() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyBigDecimal() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneBigInteger() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(10);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyBigInteger() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(11);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneEnum() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(12);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyEnums() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(13);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneShort() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(14);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyShorts() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(15);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneDate() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(16);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyDates() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(17);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_OneString() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(18);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeTestContainer_ManyString() {
		return (EAttribute)attributeTestContainerEClass.getEStructuralFeatures().get(19);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getNameAttributeContainer() {
		return nameAttributeContainerEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getNameAttributeContainer_DefaultNameTest() {
		return (EReference)nameAttributeContainerEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getNameAttributeContainer_CustomNameTest() {
		return (EReference)nameAttributeContainerEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getNameAttributeContainer_Name() {
		return (EAttribute)nameAttributeContainerEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getNameAttributeContainer_NoNameTest() {
		return (EReference)nameAttributeContainerEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDefaultName() {
		return defaultNameEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getDefaultName_Name() {
		return (EAttribute)defaultNameEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCustomName() {
		return customNameEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getCustomName_Id() {
		return (EAttribute)customNameEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getNoName() {
		return noNameEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EEnum getSampleEnum() {
		return sampleEnumEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestmodelFactory getTestmodelFactory() {
		return (TestmodelFactory)getEFactoryInstance();
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
		testModelEClass = createEClass(TEST_MODEL);
		createEReference(testModelEClass, TEST_MODEL__CHILDREN);
		createEReference(testModelEClass, TEST_MODEL__SINGLE_OPTIONAL);
		createEReference(testModelEClass, TEST_MODEL__SINGLE_REQUIRED);
		createEAttribute(testModelEClass, TEST_MODEL__NAME);
		createEReference(testModelEClass, TEST_MODEL__ATTRIBUTE_SAMPLE);
		createEReference(testModelEClass, TEST_MODEL__LINK_TO_SINGLE_REQUIRED);
		createEReference(testModelEClass, TEST_MODEL__REFERENCE_TEST_CONTAINER);
		createEReference(testModelEClass, TEST_MODEL__ATTRIBUTE_TEST);
		createEReference(testModelEClass, TEST_MODEL__NAME_ATTRIBUTE_TEST);
		createEReference(testModelEClass, TEST_MODEL__SUB);

		childEClass = createEClass(CHILD);
		createEReference(childEClass, CHILD__REFERENCE_LIST);

		singleOptionalEClass = createEClass(SINGLE_OPTIONAL);

		singleRequiredEClass = createEClass(SINGLE_REQUIRED);
		createEReference(singleRequiredEClass, SINGLE_REQUIRED__NESTED_ELEMENTS);
		createEReference(singleRequiredEClass, SINGLE_REQUIRED__NESTED_ELEMENT);
		createEAttribute(singleRequiredEClass, SINGLE_REQUIRED__NAME);

		interfaceEClass = createEClass(INTERFACE);

		abstractEClass = createEClass(ABSTRACT);
		createEAttribute(abstractEClass, ABSTRACT__PARENT_ATTRIBUTE);
		createEReference(abstractEClass, ABSTRACT__PARENT_REFERENCE);
		createEReference(abstractEClass, ABSTRACT__PARENT_CONTAINMENT);

		nestedElementsEClass = createEClass(NESTED_ELEMENTS);

		nestedElementEClass = createEClass(NESTED_ELEMENT);
		createEAttribute(nestedElementEClass, NESTED_ELEMENT__ID);

		attributeSampleEClass = createEClass(ATTRIBUTE_SAMPLE);
		createEAttribute(attributeSampleEClass, ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL);
		createEAttribute(attributeSampleEClass, ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED);
		createEAttribute(attributeSampleEClass, ATTRIBUTE_SAMPLE__BIG_DECIMAL);
		createEAttribute(attributeSampleEClass, ATTRIBUTE_SAMPLE__BIG_INTEGER);
		createEAttribute(attributeSampleEClass, ATTRIBUTE_SAMPLE__DOUBLE);

		referenceTestContainerEClass = createEClass(REFERENCE_TEST_CONTAINER);
		createEReference(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__CONTAINMENTS);
		createEReference(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE);
		createEReference(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY);
		createEAttribute(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__NAME);
		createEReference(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__CONTAINMENT);
		createEAttribute(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE);
		createEAttribute(referenceTestContainerEClass, REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES);

		referenceTargetEClass = createEClass(REFERENCE_TARGET);
		createEAttribute(referenceTargetEClass, REFERENCE_TARGET__NAME);

		referenceTargetSubclassEClass = createEClass(REFERENCE_TARGET_SUBCLASS);

		attributeTestContainerEClass = createEClass(ATTRIBUTE_TEST_CONTAINER);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__NAME);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_INT);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_INT);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_BOOL);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_BOOL);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_ENUM);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_SHORT);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_DATE);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_DATES);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__ONE_STRING);
		createEAttribute(attributeTestContainerEClass, ATTRIBUTE_TEST_CONTAINER__MANY_STRING);

		nameAttributeContainerEClass = createEClass(NAME_ATTRIBUTE_CONTAINER);
		createEReference(nameAttributeContainerEClass, NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST);
		createEReference(nameAttributeContainerEClass, NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST);
		createEAttribute(nameAttributeContainerEClass, NAME_ATTRIBUTE_CONTAINER__NAME);
		createEReference(nameAttributeContainerEClass, NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST);

		defaultNameEClass = createEClass(DEFAULT_NAME);
		createEAttribute(defaultNameEClass, DEFAULT_NAME__NAME);

		customNameEClass = createEClass(CUSTOM_NAME);
		createEAttribute(customNameEClass, CUSTOM_NAME__ID);

		noNameEClass = createEClass(NO_NAME);

		// Create enums
		sampleEnumEEnum = createEEnum(SAMPLE_ENUM);
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
		TestsubmodelPackage theTestsubmodelPackage = (TestsubmodelPackage)EPackage.Registry.INSTANCE.getEPackage(TestsubmodelPackage.eNS_URI);
		TestsubmodelWithTraditionalURIPackage theTestsubmodelWithTraditionalURIPackage = (TestsubmodelWithTraditionalURIPackage)EPackage.Registry.INSTANCE.getEPackage(TestsubmodelWithTraditionalURIPackage.eNS_URI);

		// Add subpackages
		getESubpackages().add(theTestsubmodelPackage);
		getESubpackages().add(theTestsubmodelWithTraditionalURIPackage);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		singleOptionalEClass.getESuperTypes().add(this.getInterface());
		singleRequiredEClass.getESuperTypes().add(this.getAbstract());
		singleRequiredEClass.getESuperTypes().add(this.getSingleOptional());
		referenceTargetSubclassEClass.getESuperTypes().add(this.getReferenceTarget());

		// Initialize classes and features; add operations and parameters
		initEClass(testModelEClass, TestModel.class, "TestModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestModel_Children(), this.getChild(), null, "children", null, 0, -1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_SingleOptional(), this.getSingleOptional(), null, "singleOptional", null, 0, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_SingleRequired(), this.getSingleRequired(), null, "singleRequired", null, 1, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTestModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_AttributeSample(), this.getAttributeSample(), null, "attributeSample", null, 0, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_LinkToSingleRequired(), this.getSingleRequired(), null, "linkToSingleRequired", null, 0, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_ReferenceTestContainer(), this.getReferenceTestContainer(), null, "referenceTestContainer", null, 0, -1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_AttributeTest(), this.getAttributeTestContainer(), null, "attributeTest", null, 0, -1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_NameAttributeTest(), this.getNameAttributeContainer(), null, "nameAttributeTest", null, 0, -1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTestModel_Sub(), theTestsubmodelPackage.getTestInnerModel(), null, "sub", null, 0, 1, TestModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(childEClass, Child.class, "Child", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getChild_ReferenceList(), this.getSingleRequired(), null, "referenceList", null, 0, -1, Child.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(singleOptionalEClass, SingleOptional.class, "SingleOptional", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(singleRequiredEClass, SingleRequired.class, "SingleRequired", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSingleRequired_NestedElements(), this.getNestedElements(), null, "nestedElements", null, 0, -1, SingleRequired.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSingleRequired_NestedElement(), this.getNestedElement(), null, "nestedElement", null, 0, 1, SingleRequired.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSingleRequired_Name(), ecorePackage.getEString(), "name", null, 0, 1, SingleRequired.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(interfaceEClass, Interface.class, "Interface", IS_ABSTRACT, IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(abstractEClass, Abstract.class, "Abstract", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getAbstract_ParentAttribute(), ecorePackage.getEBoolean(), "parentAttribute", null, 0, 1, Abstract.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAbstract_ParentReference(), this.getTestModel(), null, "parentReference", null, 0, 1, Abstract.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAbstract_ParentContainment(), this.getInterface(), null, "parentContainment", null, 0, 1, Abstract.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(nestedElementsEClass, NestedElements.class, "NestedElements", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(nestedElementEClass, NestedElement.class, "NestedElement", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getNestedElement_Id(), ecorePackage.getEString(), "id", null, 0, 1, NestedElement.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(attributeSampleEClass, AttributeSample.class, "AttributeSample", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getAttributeSample_SingleIntOptional(), ecorePackage.getEInt(), "singleIntOptional", null, 0, 1, AttributeSample.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeSample_ListIntRequired(), ecorePackage.getEInt(), "listIntRequired", null, 1, -1, AttributeSample.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeSample_BigDecimal(), ecorePackage.getEBigDecimal(), "bigDecimal", null, 0, 1, AttributeSample.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeSample_BigInteger(), ecorePackage.getEBigInteger(), "bigInteger", null, 0, 1, AttributeSample.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeSample_Double(), ecorePackage.getEDouble(), "double", null, 0, 1, AttributeSample.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(referenceTestContainerEClass, ReferenceTestContainer.class, "ReferenceTestContainer", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReferenceTestContainer_Containments(), this.getReferenceTarget(), null, "containments", null, 0, -1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getReferenceTestContainer_ReferenceToOne(), this.getReferenceTarget(), null, "referenceToOne", null, 0, 1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getReferenceTestContainer_ReferenceToMany(), this.getReferenceTarget(), null, "referenceToMany", null, 0, -1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReferenceTestContainer_Name(), ecorePackage.getEString(), "name", null, 0, 1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getReferenceTestContainer_Containment(), this.getReferenceTarget(), null, "containment", null, 0, 1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReferenceTestContainer_OneAttribute(), ecorePackage.getEBoolean(), "oneAttribute", null, 0, 1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReferenceTestContainer_ManyAttributes(), ecorePackage.getEBoolean(), "manyAttributes", null, 0, -1, ReferenceTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(referenceTargetEClass, ReferenceTarget.class, "ReferenceTarget", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getReferenceTarget_Name(), ecorePackage.getEString(), "name", null, 0, 1, ReferenceTarget.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(referenceTargetSubclassEClass, ReferenceTargetSubclass.class, "ReferenceTargetSubclass", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(attributeTestContainerEClass, AttributeTestContainer.class, "AttributeTestContainer", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getAttributeTestContainer_Name(), ecorePackage.getEString(), "name", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneInt(), ecorePackage.getEInt(), "oneInt", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneInteger(), ecorePackage.getEIntegerObject(), "oneInteger", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyInt(), ecorePackage.getEInt(), "manyInt", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneBool(), ecorePackage.getEBoolean(), "oneBool", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyBool(), ecorePackage.getEBoolean(), "manyBool", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, !IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneDouble(), ecorePackage.getEDouble(), "oneDouble", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyDouble(), ecorePackage.getEDouble(), "manyDouble", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneBigDecimal(), ecorePackage.getEBigDecimal(), "oneBigDecimal", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyBigDecimal(), ecorePackage.getEBigDecimal(), "manyBigDecimal", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneBigInteger(), ecorePackage.getEBigInteger(), "oneBigInteger", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyBigInteger(), ecorePackage.getEBigInteger(), "manyBigInteger", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneEnum(), this.getSampleEnum(), "oneEnum", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyEnums(), this.getSampleEnum(), "manyEnums", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneShort(), ecorePackage.getEShort(), "oneShort", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyShorts(), ecorePackage.getEShort(), "manyShorts", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneDate(), ecorePackage.getEDate(), "oneDate", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyDates(), ecorePackage.getEDate(), "manyDates", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_OneString(), ecorePackage.getEString(), "oneString", null, 0, 1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeTestContainer_ManyString(), ecorePackage.getEString(), "manyString", null, 0, -1, AttributeTestContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(nameAttributeContainerEClass, NameAttributeContainer.class, "NameAttributeContainer", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getNameAttributeContainer_DefaultNameTest(), this.getDefaultName(), null, "defaultNameTest", null, 0, 1, NameAttributeContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getNameAttributeContainer_CustomNameTest(), this.getCustomName(), null, "customNameTest", null, 0, 1, NameAttributeContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getNameAttributeContainer_Name(), ecorePackage.getEString(), "name", null, 0, 1, NameAttributeContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getNameAttributeContainer_NoNameTest(), this.getNoName(), null, "noNameTest", null, 0, 1, NameAttributeContainer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(defaultNameEClass, DefaultName.class, "DefaultName", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getDefaultName_Name(), ecorePackage.getEString(), "name", null, 0, 1, DefaultName.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(customNameEClass, CustomName.class, "CustomName", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCustomName_Id(), ecorePackage.getEString(), "id", null, 0, 1, CustomName.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(noNameEClass, NoName.class, "NoName", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		// Initialize enums and add enum literals
		initEEnum(sampleEnumEEnum, SampleEnum.class, "SampleEnum");
		addEEnumLiteral(sampleEnumEEnum, SampleEnum.SAMPLE);
		addEEnumLiteral(sampleEnumEEnum, SampleEnum.SAMPLE2);

		// Create resource
		createResource(eNS_URI);
	}

} //TestmodelPackageImpl
