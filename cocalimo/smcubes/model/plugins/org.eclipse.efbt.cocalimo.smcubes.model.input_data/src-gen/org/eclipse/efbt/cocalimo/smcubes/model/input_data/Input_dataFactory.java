/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataPackage
 * @generated
 */
public interface Input_dataFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Input_dataFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataFactoryImpl.init();

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
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Input_dataPackage getInput_dataPackage();

} //Input_dataFactory
