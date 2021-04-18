/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage
 * @generated
 */
public interface Test_input_dataFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Test_input_dataFactory eINSTANCE = org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Input Data Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Input Data Module</em>'.
	 * @generated
	 */
	InputDataModule createInputDataModule();

	/**
	 * Returns a new object of class '<em>Useful Input Data Snippets</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Useful Input Data Snippets</em>'.
	 * @generated
	 */
	UsefulInputDataSnippets createUsefulInputDataSnippets();

	/**
	 * Returns a new object of class '<em>Test Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Input Data</em>'.
	 * @generated
	 */
	TestInputData createTestInputData();

	/**
	 * Returns a new object of class '<em>External Test Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>External Test Data</em>'.
	 * @generated
	 */
	ExternalTestData createExternalTestData();

	/**
	 * Returns a new object of class '<em>Useful Input Data Snippets Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Useful Input Data Snippets Module</em>'.
	 * @generated
	 */
	UsefulInputDataSnippetsModule createUsefulInputDataSnippetsModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Test_input_dataPackage getTest_input_dataPackage();

} //Test_input_dataFactory
