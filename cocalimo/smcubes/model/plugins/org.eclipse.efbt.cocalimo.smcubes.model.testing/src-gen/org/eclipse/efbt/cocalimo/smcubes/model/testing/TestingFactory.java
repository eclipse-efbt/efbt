/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage
 * @generated
 */
public interface TestingFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestingFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Cell With Enumerated Value</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell With Enumerated Value</em>'.
	 * @generated
	 */
	CellWithEnumeratedValue createCellWithEnumeratedValue();

	/**
	 * Returns a new object of class '<em>Cell With Value</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell With Value</em>'.
	 * @generated
	 */
	CellWithValue createCellWithValue();

	/**
	 * Returns a new object of class '<em>Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Input Data</em>'.
	 * @generated
	 */
	InputData createInputData();

	/**
	 * Returns a new object of class '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell</em>'.
	 * @generated
	 */
	Cell createCell();

	/**
	 * Returns a new object of class '<em>Cube Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Data</em>'.
	 * @generated
	 */
	CubeData createCubeData();

	/**
	 * Returns a new object of class '<em>Row Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Data</em>'.
	 * @generated
	 */
	RowData createRowData();

	/**
	 * Returns a new object of class '<em>Test Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Module</em>'.
	 * @generated
	 */
	TestModule createTestModule();

	/**
	 * Returns a new object of class '<em>Report Results</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Results</em>'.
	 * @generated
	 */
	ReportResults createReportResults();

	/**
	 * Returns a new object of class '<em>Test</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test</em>'.
	 * @generated
	 */
	Test createTest();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TestingPackage getTestingPackage();

} //TestingFactory
