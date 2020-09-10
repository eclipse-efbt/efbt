/**
 */
package testmodel;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see testmodel.TestmodelPackage
 * @generated
 */
public interface TestmodelFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestmodelFactory eINSTANCE = testmodel.impl.TestmodelFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Test Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Model</em>'.
	 * @generated
	 */
	TestModel createTestModel();

	/**
	 * Returns a new object of class '<em>Child</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Child</em>'.
	 * @generated
	 */
	Child createChild();

	/**
	 * Returns a new object of class '<em>Single Optional</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Single Optional</em>'.
	 * @generated
	 */
	SingleOptional createSingleOptional();

	/**
	 * Returns a new object of class '<em>Single Required</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Single Required</em>'.
	 * @generated
	 */
	SingleRequired createSingleRequired();

	/**
	 * Returns a new object of class '<em>Nested Elements</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Nested Elements</em>'.
	 * @generated
	 */
	NestedElements createNestedElements();

	/**
	 * Returns a new object of class '<em>Nested Element</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Nested Element</em>'.
	 * @generated
	 */
	NestedElement createNestedElement();

	/**
	 * Returns a new object of class '<em>Attribute Sample</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute Sample</em>'.
	 * @generated
	 */
	AttributeSample createAttributeSample();

	/**
	 * Returns a new object of class '<em>Reference Test Container</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Reference Test Container</em>'.
	 * @generated
	 */
	ReferenceTestContainer createReferenceTestContainer();

	/**
	 * Returns a new object of class '<em>Reference Target</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Reference Target</em>'.
	 * @generated
	 */
	ReferenceTarget createReferenceTarget();

	/**
	 * Returns a new object of class '<em>Reference Target Subclass</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Reference Target Subclass</em>'.
	 * @generated
	 */
	ReferenceTargetSubclass createReferenceTargetSubclass();

	/**
	 * Returns a new object of class '<em>Attribute Test Container</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute Test Container</em>'.
	 * @generated
	 */
	AttributeTestContainer createAttributeTestContainer();

	/**
	 * Returns a new object of class '<em>Name Attribute Container</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Name Attribute Container</em>'.
	 * @generated
	 */
	NameAttributeContainer createNameAttributeContainer();

	/**
	 * Returns a new object of class '<em>Default Name</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Default Name</em>'.
	 * @generated
	 */
	DefaultName createDefaultName();

	/**
	 * Returns a new object of class '<em>Custom Name</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Custom Name</em>'.
	 * @generated
	 */
	CustomName createCustomName();

	/**
	 * Returns a new object of class '<em>No Name</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>No Name</em>'.
	 * @generated
	 */
	NoName createNoName();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TestmodelPackage getTestmodelPackage();

} //TestmodelFactory
