/**
 */
package testmodel.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import testmodel.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestmodelFactoryImpl extends EFactoryImpl implements TestmodelFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TestmodelFactory init() {
		try {
			TestmodelFactory theTestmodelFactory = (TestmodelFactory)EPackage.Registry.INSTANCE.getEFactory(TestmodelPackage.eNS_URI);
			if (theTestmodelFactory != null) {
				return theTestmodelFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestmodelFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestmodelFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case TestmodelPackage.TEST_MODEL: return createTestModel();
			case TestmodelPackage.CHILD: return createChild();
			case TestmodelPackage.SINGLE_OPTIONAL: return createSingleOptional();
			case TestmodelPackage.SINGLE_REQUIRED: return createSingleRequired();
			case TestmodelPackage.NESTED_ELEMENTS: return createNestedElements();
			case TestmodelPackage.NESTED_ELEMENT: return createNestedElement();
			case TestmodelPackage.ATTRIBUTE_SAMPLE: return createAttributeSample();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER: return createReferenceTestContainer();
			case TestmodelPackage.REFERENCE_TARGET: return createReferenceTarget();
			case TestmodelPackage.REFERENCE_TARGET_SUBCLASS: return createReferenceTargetSubclass();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER: return createAttributeTestContainer();
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER: return createNameAttributeContainer();
			case TestmodelPackage.DEFAULT_NAME: return createDefaultName();
			case TestmodelPackage.CUSTOM_NAME: return createCustomName();
			case TestmodelPackage.NO_NAME: return createNoName();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case TestmodelPackage.SAMPLE_ENUM:
				return createSampleEnumFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case TestmodelPackage.SAMPLE_ENUM:
				return convertSampleEnumToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModel createTestModel() {
		TestModelImpl testModel = new TestModelImpl();
		return testModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Child createChild() {
		ChildImpl child = new ChildImpl();
		return child;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleOptional createSingleOptional() {
		SingleOptionalImpl singleOptional = new SingleOptionalImpl();
		return singleOptional;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleRequired createSingleRequired() {
		SingleRequiredImpl singleRequired = new SingleRequiredImpl();
		return singleRequired;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NestedElements createNestedElements() {
		NestedElementsImpl nestedElements = new NestedElementsImpl();
		return nestedElements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NestedElement createNestedElement() {
		NestedElementImpl nestedElement = new NestedElementImpl();
		return nestedElement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeSample createAttributeSample() {
		AttributeSampleImpl attributeSample = new AttributeSampleImpl();
		return attributeSample;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTestContainer createReferenceTestContainer() {
		ReferenceTestContainerImpl referenceTestContainer = new ReferenceTestContainerImpl();
		return referenceTestContainer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTarget createReferenceTarget() {
		ReferenceTargetImpl referenceTarget = new ReferenceTargetImpl();
		return referenceTarget;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTargetSubclass createReferenceTargetSubclass() {
		ReferenceTargetSubclassImpl referenceTargetSubclass = new ReferenceTargetSubclassImpl();
		return referenceTargetSubclass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeTestContainer createAttributeTestContainer() {
		AttributeTestContainerImpl attributeTestContainer = new AttributeTestContainerImpl();
		return attributeTestContainer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NameAttributeContainer createNameAttributeContainer() {
		NameAttributeContainerImpl nameAttributeContainer = new NameAttributeContainerImpl();
		return nameAttributeContainer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DefaultName createDefaultName() {
		DefaultNameImpl defaultName = new DefaultNameImpl();
		return defaultName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CustomName createCustomName() {
		CustomNameImpl customName = new CustomNameImpl();
		return customName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NoName createNoName() {
		NoNameImpl noName = new NoNameImpl();
		return noName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SampleEnum createSampleEnumFromString(EDataType eDataType, String initialValue) {
		SampleEnum result = SampleEnum.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertSampleEnumToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestmodelPackage getTestmodelPackage() {
		return (TestmodelPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TestmodelPackage getPackage() {
		return TestmodelPackage.eINSTANCE;
	}

} //TestmodelFactoryImpl
