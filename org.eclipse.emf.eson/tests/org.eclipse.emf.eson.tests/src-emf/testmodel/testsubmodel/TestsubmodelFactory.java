/**
 */
package testmodel.testsubmodel;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see testmodel.testsubmodel.TestsubmodelPackage
 * @generated
 */
public interface TestsubmodelFactory extends EFactory {
    /**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    TestsubmodelFactory eINSTANCE = testmodel.testsubmodel.impl.TestsubmodelFactoryImpl.init();

    /**
	 * Returns a new object of class '<em>Test Inner Model</em>'.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Inner Model</em>'.
	 * @generated
	 */
    TestInnerModel createTestInnerModel();

    /**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
    TestsubmodelPackage getTestsubmodelPackage();

} //TestsubmodelFactory
