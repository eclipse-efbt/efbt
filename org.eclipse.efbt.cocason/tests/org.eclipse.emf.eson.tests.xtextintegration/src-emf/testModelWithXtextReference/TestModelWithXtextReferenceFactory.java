/**
 */
package testModelWithXtextReference;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see testModelWithXtextReference.TestModelWithXtextReferencePackage
 * @generated
 */
public interface TestModelWithXtextReferenceFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestModelWithXtextReferenceFactory eINSTANCE = testModelWithXtextReference.impl.TestModelWithXtextReferenceFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Test Model With Xtext Reference</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Model With Xtext Reference</em>'.
	 * @generated
	 */
	TestModelWithXtextReference createTestModelWithXtextReference();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TestModelWithXtextReferencePackage getTestModelWithXtextReferencePackage();

} //TestModelWithXtextReferenceFactory
