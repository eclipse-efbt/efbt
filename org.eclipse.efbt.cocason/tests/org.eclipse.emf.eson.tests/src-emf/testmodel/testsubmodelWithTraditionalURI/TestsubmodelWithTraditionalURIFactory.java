/**
 */
package testmodel.testsubmodelWithTraditionalURI;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIPackage
 * @generated
 */
public interface TestsubmodelWithTraditionalURIFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestsubmodelWithTraditionalURIFactory eINSTANCE = testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Test Inner Model In Package With Traditional URI</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Inner Model In Package With Traditional URI</em>'.
	 * @generated
	 */
	TestInnerModelInPackageWithTraditionalURI createTestInnerModelInPackageWithTraditionalURI();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TestsubmodelWithTraditionalURIPackage getTestsubmodelWithTraditionalURIPackage();

} //TestsubmodelWithTraditionalURIFactory
