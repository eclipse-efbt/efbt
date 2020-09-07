/**
 */
package testmodel;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see testmodel.TestmodelFactory
 * @model kind="package"
 * @generated
 */
public interface TestmodelPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "testmodel";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "testmodel";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "testmodel";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestmodelPackage eINSTANCE = testmodel.impl.TestmodelPackageImpl.init();

	/**
	 * The meta object id for the '{@link testmodel.impl.TestModelImpl <em>Test Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.TestModelImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getTestModel()
	 * @generated
	 */
	int TEST_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Children</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__CHILDREN = 0;

	/**
	 * The feature id for the '<em><b>Single Optional</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__SINGLE_OPTIONAL = 1;

	/**
	 * The feature id for the '<em><b>Single Required</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__SINGLE_REQUIRED = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__NAME = 3;

	/**
	 * The feature id for the '<em><b>Attribute Sample</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__ATTRIBUTE_SAMPLE = 4;

	/**
	 * The feature id for the '<em><b>Link To Single Required</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__LINK_TO_SINGLE_REQUIRED = 5;

	/**
	 * The feature id for the '<em><b>Reference Test Container</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__REFERENCE_TEST_CONTAINER = 6;

	/**
	 * The feature id for the '<em><b>Attribute Test</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__ATTRIBUTE_TEST = 7;

	/**
	 * The feature id for the '<em><b>Name Attribute Test</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__NAME_ATTRIBUTE_TEST = 8;

	/**
	 * The feature id for the '<em><b>Sub</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL__SUB = 9;

	/**
	 * The number of structural features of the '<em>Test Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL_FEATURE_COUNT = 10;

	/**
	 * The meta object id for the '{@link testmodel.impl.ChildImpl <em>Child</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.ChildImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getChild()
	 * @generated
	 */
	int CHILD = 1;

	/**
	 * The feature id for the '<em><b>Reference List</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHILD__REFERENCE_LIST = 0;

	/**
	 * The number of structural features of the '<em>Child</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHILD_FEATURE_COUNT = 1;

	/**
	 * The meta object id for the '{@link testmodel.Interface <em>Interface</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.Interface
	 * @see testmodel.impl.TestmodelPackageImpl#getInterface()
	 * @generated
	 */
	int INTERFACE = 4;

	/**
	 * The number of structural features of the '<em>Interface</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_FEATURE_COUNT = 0;

	/**
	 * The meta object id for the '{@link testmodel.impl.SingleOptionalImpl <em>Single Optional</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.SingleOptionalImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getSingleOptional()
	 * @generated
	 */
	int SINGLE_OPTIONAL = 2;

	/**
	 * The number of structural features of the '<em>Single Optional</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_OPTIONAL_FEATURE_COUNT = INTERFACE_FEATURE_COUNT + 0;

	/**
	 * The meta object id for the '{@link testmodel.impl.AbstractImpl <em>Abstract</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.AbstractImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getAbstract()
	 * @generated
	 */
	int ABSTRACT = 5;

	/**
	 * The feature id for the '<em><b>Parent Attribute</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT__PARENT_ATTRIBUTE = 0;

	/**
	 * The feature id for the '<em><b>Parent Reference</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT__PARENT_REFERENCE = 1;

	/**
	 * The feature id for the '<em><b>Parent Containment</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT__PARENT_CONTAINMENT = 2;

	/**
	 * The number of structural features of the '<em>Abstract</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_FEATURE_COUNT = 3;

	/**
	 * The meta object id for the '{@link testmodel.impl.SingleRequiredImpl <em>Single Required</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.SingleRequiredImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getSingleRequired()
	 * @generated
	 */
	int SINGLE_REQUIRED = 3;

	/**
	 * The feature id for the '<em><b>Parent Attribute</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__PARENT_ATTRIBUTE = ABSTRACT__PARENT_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Parent Reference</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__PARENT_REFERENCE = ABSTRACT__PARENT_REFERENCE;

	/**
	 * The feature id for the '<em><b>Parent Containment</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__PARENT_CONTAINMENT = ABSTRACT__PARENT_CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Nested Elements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__NESTED_ELEMENTS = ABSTRACT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Nested Element</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__NESTED_ELEMENT = ABSTRACT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED__NAME = ABSTRACT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Single Required</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_REQUIRED_FEATURE_COUNT = ABSTRACT_FEATURE_COUNT + 3;

	/**
	 * The meta object id for the '{@link testmodel.impl.NestedElementsImpl <em>Nested Elements</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.NestedElementsImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getNestedElements()
	 * @generated
	 */
	int NESTED_ELEMENTS = 6;

	/**
	 * The number of structural features of the '<em>Nested Elements</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NESTED_ELEMENTS_FEATURE_COUNT = 0;

	/**
	 * The meta object id for the '{@link testmodel.impl.NestedElementImpl <em>Nested Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.NestedElementImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getNestedElement()
	 * @generated
	 */
	int NESTED_ELEMENT = 7;

	/**
	 * The feature id for the '<em><b>Id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NESTED_ELEMENT__ID = 0;

	/**
	 * The number of structural features of the '<em>Nested Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NESTED_ELEMENT_FEATURE_COUNT = 1;

	/**
	 * The meta object id for the '{@link testmodel.impl.AttributeSampleImpl <em>Attribute Sample</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.AttributeSampleImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getAttributeSample()
	 * @generated
	 */
	int ATTRIBUTE_SAMPLE = 8;

	/**
	 * The feature id for the '<em><b>Single Int Optional</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL = 0;

	/**
	 * The feature id for the '<em><b>List Int Required</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED = 1;

	/**
	 * The feature id for the '<em><b>Big Decimal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE__BIG_DECIMAL = 2;

	/**
	 * The feature id for the '<em><b>Big Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE__BIG_INTEGER = 3;

	/**
	 * The feature id for the '<em><b>Double</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE__DOUBLE = 4;

	/**
	 * The number of structural features of the '<em>Attribute Sample</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_SAMPLE_FEATURE_COUNT = 5;

	/**
	 * The meta object id for the '{@link testmodel.impl.ReferenceTestContainerImpl <em>Reference Test Container</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.ReferenceTestContainerImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTestContainer()
	 * @generated
	 */
	int REFERENCE_TEST_CONTAINER = 9;

	/**
	 * The feature id for the '<em><b>Containments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__CONTAINMENTS = 0;

	/**
	 * The feature id for the '<em><b>Reference To One</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE = 1;

	/**
	 * The feature id for the '<em><b>Reference To Many</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__NAME = 3;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__CONTAINMENT = 4;

	/**
	 * The feature id for the '<em><b>One Attribute</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE = 5;

	/**
	 * The feature id for the '<em><b>Many Attributes</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES = 6;

	/**
	 * The number of structural features of the '<em>Reference Test Container</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TEST_CONTAINER_FEATURE_COUNT = 7;

	/**
	 * The meta object id for the '{@link testmodel.impl.ReferenceTargetImpl <em>Reference Target</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.ReferenceTargetImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTarget()
	 * @generated
	 */
	int REFERENCE_TARGET = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TARGET__NAME = 0;

	/**
	 * The number of structural features of the '<em>Reference Target</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TARGET_FEATURE_COUNT = 1;

	/**
	 * The meta object id for the '{@link testmodel.impl.ReferenceTargetSubclassImpl <em>Reference Target Subclass</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.ReferenceTargetSubclassImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTargetSubclass()
	 * @generated
	 */
	int REFERENCE_TARGET_SUBCLASS = 11;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TARGET_SUBCLASS__NAME = REFERENCE_TARGET__NAME;

	/**
	 * The number of structural features of the '<em>Reference Target Subclass</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REFERENCE_TARGET_SUBCLASS_FEATURE_COUNT = REFERENCE_TARGET_FEATURE_COUNT + 0;

	/**
	 * The meta object id for the '{@link testmodel.impl.AttributeTestContainerImpl <em>Attribute Test Container</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.AttributeTestContainerImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getAttributeTestContainer()
	 * @generated
	 */
	int ATTRIBUTE_TEST_CONTAINER = 12;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__NAME = 0;

	/**
	 * The feature id for the '<em><b>One Int</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_INT = 1;

	/**
	 * The feature id for the '<em><b>One Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER = 2;

	/**
	 * The feature id for the '<em><b>Many Int</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_INT = 3;

	/**
	 * The feature id for the '<em><b>One Bool</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_BOOL = 4;

	/**
	 * The feature id for the '<em><b>Many Bool</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_BOOL = 5;

	/**
	 * The feature id for the '<em><b>One Double</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE = 6;

	/**
	 * The feature id for the '<em><b>Many Double</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE = 7;

	/**
	 * The feature id for the '<em><b>One Big Decimal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL = 8;

	/**
	 * The feature id for the '<em><b>Many Big Decimal</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL = 9;

	/**
	 * The feature id for the '<em><b>One Big Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER = 10;

	/**
	 * The feature id for the '<em><b>Many Big Integer</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER = 11;

	/**
	 * The feature id for the '<em><b>One Enum</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_ENUM = 12;

	/**
	 * The feature id for the '<em><b>Many Enums</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS = 13;

	/**
	 * The feature id for the '<em><b>One Short</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_SHORT = 14;

	/**
	 * The feature id for the '<em><b>Many Shorts</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS = 15;

	/**
	 * The feature id for the '<em><b>One Date</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_DATE = 16;

	/**
	 * The feature id for the '<em><b>Many Dates</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_DATES = 17;

	/**
	 * The feature id for the '<em><b>One String</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__ONE_STRING = 18;

	/**
	 * The feature id for the '<em><b>Many String</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER__MANY_STRING = 19;

	/**
	 * The number of structural features of the '<em>Attribute Test Container</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_TEST_CONTAINER_FEATURE_COUNT = 20;

	/**
	 * The meta object id for the '{@link testmodel.impl.NameAttributeContainerImpl <em>Name Attribute Container</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.NameAttributeContainerImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getNameAttributeContainer()
	 * @generated
	 */
	int NAME_ATTRIBUTE_CONTAINER = 13;

	/**
	 * The feature id for the '<em><b>Default Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST = 0;

	/**
	 * The feature id for the '<em><b>Custom Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAME_ATTRIBUTE_CONTAINER__NAME = 2;

	/**
	 * The feature id for the '<em><b>No Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST = 3;

	/**
	 * The number of structural features of the '<em>Name Attribute Container</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAME_ATTRIBUTE_CONTAINER_FEATURE_COUNT = 4;

	/**
	 * The meta object id for the '{@link testmodel.impl.DefaultNameImpl <em>Default Name</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.DefaultNameImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getDefaultName()
	 * @generated
	 */
	int DEFAULT_NAME = 14;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DEFAULT_NAME__NAME = 0;

	/**
	 * The number of structural features of the '<em>Default Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DEFAULT_NAME_FEATURE_COUNT = 1;

	/**
	 * The meta object id for the '{@link testmodel.impl.CustomNameImpl <em>Custom Name</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.CustomNameImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getCustomName()
	 * @generated
	 */
	int CUSTOM_NAME = 15;

	/**
	 * The feature id for the '<em><b>Id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_NAME__ID = 0;

	/**
	 * The number of structural features of the '<em>Custom Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_NAME_FEATURE_COUNT = 1;

	/**
	 * The meta object id for the '{@link testmodel.impl.NoNameImpl <em>No Name</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.impl.NoNameImpl
	 * @see testmodel.impl.TestmodelPackageImpl#getNoName()
	 * @generated
	 */
	int NO_NAME = 16;

	/**
	 * The number of structural features of the '<em>No Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NO_NAME_FEATURE_COUNT = 0;

	/**
	 * The meta object id for the '{@link testmodel.SampleEnum <em>Sample Enum</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.SampleEnum
	 * @see testmodel.impl.TestmodelPackageImpl#getSampleEnum()
	 * @generated
	 */
	int SAMPLE_ENUM = 17;


	/**
	 * Returns the meta object for class '{@link testmodel.TestModel <em>Test Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Model</em>'.
	 * @see testmodel.TestModel
	 * @generated
	 */
	EClass getTestModel();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.TestModel#getChildren <em>Children</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Children</em>'.
	 * @see testmodel.TestModel#getChildren()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_Children();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.TestModel#getSingleOptional <em>Single Optional</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Single Optional</em>'.
	 * @see testmodel.TestModel#getSingleOptional()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_SingleOptional();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.TestModel#getSingleRequired <em>Single Required</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Single Required</em>'.
	 * @see testmodel.TestModel#getSingleRequired()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_SingleRequired();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.TestModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.TestModel#getName()
	 * @see #getTestModel()
	 * @generated
	 */
	EAttribute getTestModel_Name();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.TestModel#getAttributeSample <em>Attribute Sample</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Attribute Sample</em>'.
	 * @see testmodel.TestModel#getAttributeSample()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_AttributeSample();

	/**
	 * Returns the meta object for the reference '{@link testmodel.TestModel#getLinkToSingleRequired <em>Link To Single Required</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Link To Single Required</em>'.
	 * @see testmodel.TestModel#getLinkToSingleRequired()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_LinkToSingleRequired();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.TestModel#getReferenceTestContainer <em>Reference Test Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Reference Test Container</em>'.
	 * @see testmodel.TestModel#getReferenceTestContainer()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_ReferenceTestContainer();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.TestModel#getAttributeTest <em>Attribute Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attribute Test</em>'.
	 * @see testmodel.TestModel#getAttributeTest()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_AttributeTest();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.TestModel#getNameAttributeTest <em>Name Attribute Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Name Attribute Test</em>'.
	 * @see testmodel.TestModel#getNameAttributeTest()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_NameAttributeTest();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.TestModel#getSub <em>Sub</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Sub</em>'.
	 * @see testmodel.TestModel#getSub()
	 * @see #getTestModel()
	 * @generated
	 */
	EReference getTestModel_Sub();

	/**
	 * Returns the meta object for class '{@link testmodel.Child <em>Child</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Child</em>'.
	 * @see testmodel.Child
	 * @generated
	 */
	EClass getChild();

	/**
	 * Returns the meta object for the reference list '{@link testmodel.Child#getReferenceList <em>Reference List</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Reference List</em>'.
	 * @see testmodel.Child#getReferenceList()
	 * @see #getChild()
	 * @generated
	 */
	EReference getChild_ReferenceList();

	/**
	 * Returns the meta object for class '{@link testmodel.SingleOptional <em>Single Optional</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Single Optional</em>'.
	 * @see testmodel.SingleOptional
	 * @generated
	 */
	EClass getSingleOptional();

	/**
	 * Returns the meta object for class '{@link testmodel.SingleRequired <em>Single Required</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Single Required</em>'.
	 * @see testmodel.SingleRequired
	 * @generated
	 */
	EClass getSingleRequired();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.SingleRequired#getNestedElements <em>Nested Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Nested Elements</em>'.
	 * @see testmodel.SingleRequired#getNestedElements()
	 * @see #getSingleRequired()
	 * @generated
	 */
	EReference getSingleRequired_NestedElements();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.SingleRequired#getNestedElement <em>Nested Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Nested Element</em>'.
	 * @see testmodel.SingleRequired#getNestedElement()
	 * @see #getSingleRequired()
	 * @generated
	 */
	EReference getSingleRequired_NestedElement();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.SingleRequired#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.SingleRequired#getName()
	 * @see #getSingleRequired()
	 * @generated
	 */
	EAttribute getSingleRequired_Name();

	/**
	 * Returns the meta object for class '{@link testmodel.Interface <em>Interface</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Interface</em>'.
	 * @see testmodel.Interface
	 * @generated
	 */
	EClass getInterface();

	/**
	 * Returns the meta object for class '{@link testmodel.Abstract <em>Abstract</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract</em>'.
	 * @see testmodel.Abstract
	 * @generated
	 */
	EClass getAbstract();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.Abstract#isParentAttribute <em>Parent Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Parent Attribute</em>'.
	 * @see testmodel.Abstract#isParentAttribute()
	 * @see #getAbstract()
	 * @generated
	 */
	EAttribute getAbstract_ParentAttribute();

	/**
	 * Returns the meta object for the reference '{@link testmodel.Abstract#getParentReference <em>Parent Reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent Reference</em>'.
	 * @see testmodel.Abstract#getParentReference()
	 * @see #getAbstract()
	 * @generated
	 */
	EReference getAbstract_ParentReference();

	/**
	 * Returns the meta object for the reference '{@link testmodel.Abstract#getParentContainment <em>Parent Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent Containment</em>'.
	 * @see testmodel.Abstract#getParentContainment()
	 * @see #getAbstract()
	 * @generated
	 */
	EReference getAbstract_ParentContainment();

	/**
	 * Returns the meta object for class '{@link testmodel.NestedElements <em>Nested Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Nested Elements</em>'.
	 * @see testmodel.NestedElements
	 * @generated
	 */
	EClass getNestedElements();

	/**
	 * Returns the meta object for class '{@link testmodel.NestedElement <em>Nested Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Nested Element</em>'.
	 * @see testmodel.NestedElement
	 * @generated
	 */
	EClass getNestedElement();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.NestedElement#getId <em>Id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Id</em>'.
	 * @see testmodel.NestedElement#getId()
	 * @see #getNestedElement()
	 * @generated
	 */
	EAttribute getNestedElement_Id();

	/**
	 * Returns the meta object for class '{@link testmodel.AttributeSample <em>Attribute Sample</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute Sample</em>'.
	 * @see testmodel.AttributeSample
	 * @generated
	 */
	EClass getAttributeSample();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeSample#getSingleIntOptional <em>Single Int Optional</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Single Int Optional</em>'.
	 * @see testmodel.AttributeSample#getSingleIntOptional()
	 * @see #getAttributeSample()
	 * @generated
	 */
	EAttribute getAttributeSample_SingleIntOptional();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeSample#getListIntRequired <em>List Int Required</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>List Int Required</em>'.
	 * @see testmodel.AttributeSample#getListIntRequired()
	 * @see #getAttributeSample()
	 * @generated
	 */
	EAttribute getAttributeSample_ListIntRequired();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeSample#getBigDecimal <em>Big Decimal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Big Decimal</em>'.
	 * @see testmodel.AttributeSample#getBigDecimal()
	 * @see #getAttributeSample()
	 * @generated
	 */
	EAttribute getAttributeSample_BigDecimal();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeSample#getBigInteger <em>Big Integer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Big Integer</em>'.
	 * @see testmodel.AttributeSample#getBigInteger()
	 * @see #getAttributeSample()
	 * @generated
	 */
	EAttribute getAttributeSample_BigInteger();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeSample#getDouble <em>Double</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Double</em>'.
	 * @see testmodel.AttributeSample#getDouble()
	 * @see #getAttributeSample()
	 * @generated
	 */
	EAttribute getAttributeSample_Double();

	/**
	 * Returns the meta object for class '{@link testmodel.ReferenceTestContainer <em>Reference Test Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reference Test Container</em>'.
	 * @see testmodel.ReferenceTestContainer
	 * @generated
	 */
	EClass getReferenceTestContainer();

	/**
	 * Returns the meta object for the containment reference list '{@link testmodel.ReferenceTestContainer#getContainments <em>Containments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Containments</em>'.
	 * @see testmodel.ReferenceTestContainer#getContainments()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EReference getReferenceTestContainer_Containments();

	/**
	 * Returns the meta object for the reference '{@link testmodel.ReferenceTestContainer#getReferenceToOne <em>Reference To One</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Reference To One</em>'.
	 * @see testmodel.ReferenceTestContainer#getReferenceToOne()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EReference getReferenceTestContainer_ReferenceToOne();

	/**
	 * Returns the meta object for the reference list '{@link testmodel.ReferenceTestContainer#getReferenceToMany <em>Reference To Many</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Reference To Many</em>'.
	 * @see testmodel.ReferenceTestContainer#getReferenceToMany()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EReference getReferenceTestContainer_ReferenceToMany();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.ReferenceTestContainer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.ReferenceTestContainer#getName()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EAttribute getReferenceTestContainer_Name();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.ReferenceTestContainer#getContainment <em>Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Containment</em>'.
	 * @see testmodel.ReferenceTestContainer#getContainment()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EReference getReferenceTestContainer_Containment();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.ReferenceTestContainer#isOneAttribute <em>One Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Attribute</em>'.
	 * @see testmodel.ReferenceTestContainer#isOneAttribute()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EAttribute getReferenceTestContainer_OneAttribute();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.ReferenceTestContainer#getManyAttributes <em>Many Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Attributes</em>'.
	 * @see testmodel.ReferenceTestContainer#getManyAttributes()
	 * @see #getReferenceTestContainer()
	 * @generated
	 */
	EAttribute getReferenceTestContainer_ManyAttributes();

	/**
	 * Returns the meta object for class '{@link testmodel.ReferenceTarget <em>Reference Target</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reference Target</em>'.
	 * @see testmodel.ReferenceTarget
	 * @generated
	 */
	EClass getReferenceTarget();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.ReferenceTarget#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.ReferenceTarget#getName()
	 * @see #getReferenceTarget()
	 * @generated
	 */
	EAttribute getReferenceTarget_Name();

	/**
	 * Returns the meta object for class '{@link testmodel.ReferenceTargetSubclass <em>Reference Target Subclass</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reference Target Subclass</em>'.
	 * @see testmodel.ReferenceTargetSubclass
	 * @generated
	 */
	EClass getReferenceTargetSubclass();

	/**
	 * Returns the meta object for class '{@link testmodel.AttributeTestContainer <em>Attribute Test Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute Test Container</em>'.
	 * @see testmodel.AttributeTestContainer
	 * @generated
	 */
	EClass getAttributeTestContainer();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.AttributeTestContainer#getName()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_Name();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneInt <em>One Int</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Int</em>'.
	 * @see testmodel.AttributeTestContainer#getOneInt()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneInt();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneInteger <em>One Integer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Integer</em>'.
	 * @see testmodel.AttributeTestContainer#getOneInteger()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneInteger();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyInt <em>Many Int</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Int</em>'.
	 * @see testmodel.AttributeTestContainer#getManyInt()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyInt();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#isOneBool <em>One Bool</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Bool</em>'.
	 * @see testmodel.AttributeTestContainer#isOneBool()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneBool();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyBool <em>Many Bool</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Bool</em>'.
	 * @see testmodel.AttributeTestContainer#getManyBool()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyBool();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneDouble <em>One Double</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Double</em>'.
	 * @see testmodel.AttributeTestContainer#getOneDouble()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneDouble();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyDouble <em>Many Double</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Double</em>'.
	 * @see testmodel.AttributeTestContainer#getManyDouble()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyDouble();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneBigDecimal <em>One Big Decimal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Big Decimal</em>'.
	 * @see testmodel.AttributeTestContainer#getOneBigDecimal()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneBigDecimal();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyBigDecimal <em>Many Big Decimal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Big Decimal</em>'.
	 * @see testmodel.AttributeTestContainer#getManyBigDecimal()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyBigDecimal();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneBigInteger <em>One Big Integer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Big Integer</em>'.
	 * @see testmodel.AttributeTestContainer#getOneBigInteger()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneBigInteger();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyBigInteger <em>Many Big Integer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Big Integer</em>'.
	 * @see testmodel.AttributeTestContainer#getManyBigInteger()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyBigInteger();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneEnum <em>One Enum</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Enum</em>'.
	 * @see testmodel.AttributeTestContainer#getOneEnum()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneEnum();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyEnums <em>Many Enums</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Enums</em>'.
	 * @see testmodel.AttributeTestContainer#getManyEnums()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyEnums();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneShort <em>One Short</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Short</em>'.
	 * @see testmodel.AttributeTestContainer#getOneShort()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneShort();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyShorts <em>Many Shorts</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Shorts</em>'.
	 * @see testmodel.AttributeTestContainer#getManyShorts()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyShorts();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneDate <em>One Date</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One Date</em>'.
	 * @see testmodel.AttributeTestContainer#getOneDate()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneDate();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyDates <em>Many Dates</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many Dates</em>'.
	 * @see testmodel.AttributeTestContainer#getManyDates()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyDates();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.AttributeTestContainer#getOneString <em>One String</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>One String</em>'.
	 * @see testmodel.AttributeTestContainer#getOneString()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_OneString();

	/**
	 * Returns the meta object for the attribute list '{@link testmodel.AttributeTestContainer#getManyString <em>Many String</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Many String</em>'.
	 * @see testmodel.AttributeTestContainer#getManyString()
	 * @see #getAttributeTestContainer()
	 * @generated
	 */
	EAttribute getAttributeTestContainer_ManyString();

	/**
	 * Returns the meta object for class '{@link testmodel.NameAttributeContainer <em>Name Attribute Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Name Attribute Container</em>'.
	 * @see testmodel.NameAttributeContainer
	 * @generated
	 */
	EClass getNameAttributeContainer();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.NameAttributeContainer#getDefaultNameTest <em>Default Name Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Default Name Test</em>'.
	 * @see testmodel.NameAttributeContainer#getDefaultNameTest()
	 * @see #getNameAttributeContainer()
	 * @generated
	 */
	EReference getNameAttributeContainer_DefaultNameTest();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.NameAttributeContainer#getCustomNameTest <em>Custom Name Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Custom Name Test</em>'.
	 * @see testmodel.NameAttributeContainer#getCustomNameTest()
	 * @see #getNameAttributeContainer()
	 * @generated
	 */
	EReference getNameAttributeContainer_CustomNameTest();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.NameAttributeContainer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.NameAttributeContainer#getName()
	 * @see #getNameAttributeContainer()
	 * @generated
	 */
	EAttribute getNameAttributeContainer_Name();

	/**
	 * Returns the meta object for the containment reference '{@link testmodel.NameAttributeContainer#getNoNameTest <em>No Name Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>No Name Test</em>'.
	 * @see testmodel.NameAttributeContainer#getNoNameTest()
	 * @see #getNameAttributeContainer()
	 * @generated
	 */
	EReference getNameAttributeContainer_NoNameTest();

	/**
	 * Returns the meta object for class '{@link testmodel.DefaultName <em>Default Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Default Name</em>'.
	 * @see testmodel.DefaultName
	 * @generated
	 */
	EClass getDefaultName();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.DefaultName#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.DefaultName#getName()
	 * @see #getDefaultName()
	 * @generated
	 */
	EAttribute getDefaultName_Name();

	/**
	 * Returns the meta object for class '{@link testmodel.CustomName <em>Custom Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Custom Name</em>'.
	 * @see testmodel.CustomName
	 * @generated
	 */
	EClass getCustomName();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.CustomName#getId <em>Id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Id</em>'.
	 * @see testmodel.CustomName#getId()
	 * @see #getCustomName()
	 * @generated
	 */
	EAttribute getCustomName_Id();

	/**
	 * Returns the meta object for class '{@link testmodel.NoName <em>No Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>No Name</em>'.
	 * @see testmodel.NoName
	 * @generated
	 */
	EClass getNoName();

	/**
	 * Returns the meta object for enum '{@link testmodel.SampleEnum <em>Sample Enum</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Sample Enum</em>'.
	 * @see testmodel.SampleEnum
	 * @generated
	 */
	EEnum getSampleEnum();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestmodelFactory getTestmodelFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link testmodel.impl.TestModelImpl <em>Test Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.TestModelImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getTestModel()
		 * @generated
		 */
		EClass TEST_MODEL = eINSTANCE.getTestModel();

		/**
		 * The meta object literal for the '<em><b>Children</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__CHILDREN = eINSTANCE.getTestModel_Children();

		/**
		 * The meta object literal for the '<em><b>Single Optional</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__SINGLE_OPTIONAL = eINSTANCE.getTestModel_SingleOptional();

		/**
		 * The meta object literal for the '<em><b>Single Required</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__SINGLE_REQUIRED = eINSTANCE.getTestModel_SingleRequired();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_MODEL__NAME = eINSTANCE.getTestModel_Name();

		/**
		 * The meta object literal for the '<em><b>Attribute Sample</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__ATTRIBUTE_SAMPLE = eINSTANCE.getTestModel_AttributeSample();

		/**
		 * The meta object literal for the '<em><b>Link To Single Required</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__LINK_TO_SINGLE_REQUIRED = eINSTANCE.getTestModel_LinkToSingleRequired();

		/**
		 * The meta object literal for the '<em><b>Reference Test Container</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__REFERENCE_TEST_CONTAINER = eINSTANCE.getTestModel_ReferenceTestContainer();

		/**
		 * The meta object literal for the '<em><b>Attribute Test</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__ATTRIBUTE_TEST = eINSTANCE.getTestModel_AttributeTest();

		/**
		 * The meta object literal for the '<em><b>Name Attribute Test</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__NAME_ATTRIBUTE_TEST = eINSTANCE.getTestModel_NameAttributeTest();

		/**
		 * The meta object literal for the '<em><b>Sub</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL__SUB = eINSTANCE.getTestModel_Sub();

		/**
		 * The meta object literal for the '{@link testmodel.impl.ChildImpl <em>Child</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.ChildImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getChild()
		 * @generated
		 */
		EClass CHILD = eINSTANCE.getChild();

		/**
		 * The meta object literal for the '<em><b>Reference List</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CHILD__REFERENCE_LIST = eINSTANCE.getChild_ReferenceList();

		/**
		 * The meta object literal for the '{@link testmodel.impl.SingleOptionalImpl <em>Single Optional</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.SingleOptionalImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getSingleOptional()
		 * @generated
		 */
		EClass SINGLE_OPTIONAL = eINSTANCE.getSingleOptional();

		/**
		 * The meta object literal for the '{@link testmodel.impl.SingleRequiredImpl <em>Single Required</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.SingleRequiredImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getSingleRequired()
		 * @generated
		 */
		EClass SINGLE_REQUIRED = eINSTANCE.getSingleRequired();

		/**
		 * The meta object literal for the '<em><b>Nested Elements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_REQUIRED__NESTED_ELEMENTS = eINSTANCE.getSingleRequired_NestedElements();

		/**
		 * The meta object literal for the '<em><b>Nested Element</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_REQUIRED__NESTED_ELEMENT = eINSTANCE.getSingleRequired_NestedElement();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SINGLE_REQUIRED__NAME = eINSTANCE.getSingleRequired_Name();

		/**
		 * The meta object literal for the '{@link testmodel.Interface <em>Interface</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.Interface
		 * @see testmodel.impl.TestmodelPackageImpl#getInterface()
		 * @generated
		 */
		EClass INTERFACE = eINSTANCE.getInterface();

		/**
		 * The meta object literal for the '{@link testmodel.impl.AbstractImpl <em>Abstract</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.AbstractImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getAbstract()
		 * @generated
		 */
		EClass ABSTRACT = eINSTANCE.getAbstract();

		/**
		 * The meta object literal for the '<em><b>Parent Attribute</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ABSTRACT__PARENT_ATTRIBUTE = eINSTANCE.getAbstract_ParentAttribute();

		/**
		 * The meta object literal for the '<em><b>Parent Reference</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT__PARENT_REFERENCE = eINSTANCE.getAbstract_ParentReference();

		/**
		 * The meta object literal for the '<em><b>Parent Containment</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT__PARENT_CONTAINMENT = eINSTANCE.getAbstract_ParentContainment();

		/**
		 * The meta object literal for the '{@link testmodel.impl.NestedElementsImpl <em>Nested Elements</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.NestedElementsImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getNestedElements()
		 * @generated
		 */
		EClass NESTED_ELEMENTS = eINSTANCE.getNestedElements();

		/**
		 * The meta object literal for the '{@link testmodel.impl.NestedElementImpl <em>Nested Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.NestedElementImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getNestedElement()
		 * @generated
		 */
		EClass NESTED_ELEMENT = eINSTANCE.getNestedElement();

		/**
		 * The meta object literal for the '<em><b>Id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NESTED_ELEMENT__ID = eINSTANCE.getNestedElement_Id();

		/**
		 * The meta object literal for the '{@link testmodel.impl.AttributeSampleImpl <em>Attribute Sample</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.AttributeSampleImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getAttributeSample()
		 * @generated
		 */
		EClass ATTRIBUTE_SAMPLE = eINSTANCE.getAttributeSample();

		/**
		 * The meta object literal for the '<em><b>Single Int Optional</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL = eINSTANCE.getAttributeSample_SingleIntOptional();

		/**
		 * The meta object literal for the '<em><b>List Int Required</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED = eINSTANCE.getAttributeSample_ListIntRequired();

		/**
		 * The meta object literal for the '<em><b>Big Decimal</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_SAMPLE__BIG_DECIMAL = eINSTANCE.getAttributeSample_BigDecimal();

		/**
		 * The meta object literal for the '<em><b>Big Integer</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_SAMPLE__BIG_INTEGER = eINSTANCE.getAttributeSample_BigInteger();

		/**
		 * The meta object literal for the '<em><b>Double</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_SAMPLE__DOUBLE = eINSTANCE.getAttributeSample_Double();

		/**
		 * The meta object literal for the '{@link testmodel.impl.ReferenceTestContainerImpl <em>Reference Test Container</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.ReferenceTestContainerImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTestContainer()
		 * @generated
		 */
		EClass REFERENCE_TEST_CONTAINER = eINSTANCE.getReferenceTestContainer();

		/**
		 * The meta object literal for the '<em><b>Containments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REFERENCE_TEST_CONTAINER__CONTAINMENTS = eINSTANCE.getReferenceTestContainer_Containments();

		/**
		 * The meta object literal for the '<em><b>Reference To One</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE = eINSTANCE.getReferenceTestContainer_ReferenceToOne();

		/**
		 * The meta object literal for the '<em><b>Reference To Many</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY = eINSTANCE.getReferenceTestContainer_ReferenceToMany();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REFERENCE_TEST_CONTAINER__NAME = eINSTANCE.getReferenceTestContainer_Name();

		/**
		 * The meta object literal for the '<em><b>Containment</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REFERENCE_TEST_CONTAINER__CONTAINMENT = eINSTANCE.getReferenceTestContainer_Containment();

		/**
		 * The meta object literal for the '<em><b>One Attribute</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE = eINSTANCE.getReferenceTestContainer_OneAttribute();

		/**
		 * The meta object literal for the '<em><b>Many Attributes</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES = eINSTANCE.getReferenceTestContainer_ManyAttributes();

		/**
		 * The meta object literal for the '{@link testmodel.impl.ReferenceTargetImpl <em>Reference Target</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.ReferenceTargetImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTarget()
		 * @generated
		 */
		EClass REFERENCE_TARGET = eINSTANCE.getReferenceTarget();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REFERENCE_TARGET__NAME = eINSTANCE.getReferenceTarget_Name();

		/**
		 * The meta object literal for the '{@link testmodel.impl.ReferenceTargetSubclassImpl <em>Reference Target Subclass</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.ReferenceTargetSubclassImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getReferenceTargetSubclass()
		 * @generated
		 */
		EClass REFERENCE_TARGET_SUBCLASS = eINSTANCE.getReferenceTargetSubclass();

		/**
		 * The meta object literal for the '{@link testmodel.impl.AttributeTestContainerImpl <em>Attribute Test Container</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.AttributeTestContainerImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getAttributeTestContainer()
		 * @generated
		 */
		EClass ATTRIBUTE_TEST_CONTAINER = eINSTANCE.getAttributeTestContainer();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__NAME = eINSTANCE.getAttributeTestContainer_Name();

		/**
		 * The meta object literal for the '<em><b>One Int</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_INT = eINSTANCE.getAttributeTestContainer_OneInt();

		/**
		 * The meta object literal for the '<em><b>One Integer</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER = eINSTANCE.getAttributeTestContainer_OneInteger();

		/**
		 * The meta object literal for the '<em><b>Many Int</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_INT = eINSTANCE.getAttributeTestContainer_ManyInt();

		/**
		 * The meta object literal for the '<em><b>One Bool</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_BOOL = eINSTANCE.getAttributeTestContainer_OneBool();

		/**
		 * The meta object literal for the '<em><b>Many Bool</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_BOOL = eINSTANCE.getAttributeTestContainer_ManyBool();

		/**
		 * The meta object literal for the '<em><b>One Double</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE = eINSTANCE.getAttributeTestContainer_OneDouble();

		/**
		 * The meta object literal for the '<em><b>Many Double</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE = eINSTANCE.getAttributeTestContainer_ManyDouble();

		/**
		 * The meta object literal for the '<em><b>One Big Decimal</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL = eINSTANCE.getAttributeTestContainer_OneBigDecimal();

		/**
		 * The meta object literal for the '<em><b>Many Big Decimal</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL = eINSTANCE.getAttributeTestContainer_ManyBigDecimal();

		/**
		 * The meta object literal for the '<em><b>One Big Integer</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER = eINSTANCE.getAttributeTestContainer_OneBigInteger();

		/**
		 * The meta object literal for the '<em><b>Many Big Integer</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER = eINSTANCE.getAttributeTestContainer_ManyBigInteger();

		/**
		 * The meta object literal for the '<em><b>One Enum</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_ENUM = eINSTANCE.getAttributeTestContainer_OneEnum();

		/**
		 * The meta object literal for the '<em><b>Many Enums</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS = eINSTANCE.getAttributeTestContainer_ManyEnums();

		/**
		 * The meta object literal for the '<em><b>One Short</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_SHORT = eINSTANCE.getAttributeTestContainer_OneShort();

		/**
		 * The meta object literal for the '<em><b>Many Shorts</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS = eINSTANCE.getAttributeTestContainer_ManyShorts();

		/**
		 * The meta object literal for the '<em><b>One Date</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_DATE = eINSTANCE.getAttributeTestContainer_OneDate();

		/**
		 * The meta object literal for the '<em><b>Many Dates</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_DATES = eINSTANCE.getAttributeTestContainer_ManyDates();

		/**
		 * The meta object literal for the '<em><b>One String</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__ONE_STRING = eINSTANCE.getAttributeTestContainer_OneString();

		/**
		 * The meta object literal for the '<em><b>Many String</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_TEST_CONTAINER__MANY_STRING = eINSTANCE.getAttributeTestContainer_ManyString();

		/**
		 * The meta object literal for the '{@link testmodel.impl.NameAttributeContainerImpl <em>Name Attribute Container</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.NameAttributeContainerImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getNameAttributeContainer()
		 * @generated
		 */
		EClass NAME_ATTRIBUTE_CONTAINER = eINSTANCE.getNameAttributeContainer();

		/**
		 * The meta object literal for the '<em><b>Default Name Test</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST = eINSTANCE.getNameAttributeContainer_DefaultNameTest();

		/**
		 * The meta object literal for the '<em><b>Custom Name Test</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST = eINSTANCE.getNameAttributeContainer_CustomNameTest();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NAME_ATTRIBUTE_CONTAINER__NAME = eINSTANCE.getNameAttributeContainer_Name();

		/**
		 * The meta object literal for the '<em><b>No Name Test</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST = eINSTANCE.getNameAttributeContainer_NoNameTest();

		/**
		 * The meta object literal for the '{@link testmodel.impl.DefaultNameImpl <em>Default Name</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.DefaultNameImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getDefaultName()
		 * @generated
		 */
		EClass DEFAULT_NAME = eINSTANCE.getDefaultName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DEFAULT_NAME__NAME = eINSTANCE.getDefaultName_Name();

		/**
		 * The meta object literal for the '{@link testmodel.impl.CustomNameImpl <em>Custom Name</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.CustomNameImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getCustomName()
		 * @generated
		 */
		EClass CUSTOM_NAME = eINSTANCE.getCustomName();

		/**
		 * The meta object literal for the '<em><b>Id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUSTOM_NAME__ID = eINSTANCE.getCustomName_Id();

		/**
		 * The meta object literal for the '{@link testmodel.impl.NoNameImpl <em>No Name</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.impl.NoNameImpl
		 * @see testmodel.impl.TestmodelPackageImpl#getNoName()
		 * @generated
		 */
		EClass NO_NAME = eINSTANCE.getNoName();

		/**
		 * The meta object literal for the '{@link testmodel.SampleEnum <em>Sample Enum</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.SampleEnum
		 * @see testmodel.impl.TestmodelPackageImpl#getSampleEnum()
		 * @generated
		 */
		EEnum SAMPLE_ENUM = eINSTANCE.getSampleEnum();

	}

} //TestmodelPackage
