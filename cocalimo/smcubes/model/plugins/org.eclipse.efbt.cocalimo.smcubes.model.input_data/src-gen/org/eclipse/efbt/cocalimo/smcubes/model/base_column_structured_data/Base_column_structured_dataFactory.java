/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage
 * @generated
 */
public interface Base_column_structured_dataFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Base_column_structured_dataFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Base Cell With Enumerated Value</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Cell With Enumerated Value</em>'.
	 * @generated
	 */
	BaseCellWithEnumeratedValue createBaseCellWithEnumeratedValue();

	/**
	 * Returns a new object of class '<em>Base Cell With Value</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Cell With Value</em>'.
	 * @generated
	 */
	BaseCellWithValue createBaseCellWithValue();

	/**
	 * Returns a new object of class '<em>Base Column Structured Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Column Structured Data</em>'.
	 * @generated
	 */
	BaseColumnStructuredData createBaseColumnStructuredData();

	/**
	 * Returns a new object of class '<em>Base Row Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Row Data</em>'.
	 * @generated
	 */
	BaseRowData createBaseRowData();

	/**
	 * Returns a new object of class '<em>Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Input Data</em>'.
	 * @generated
	 */
	InputData createInputData();

	/**
	 * Returns a new object of class '<em>Input Data Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Input Data Module</em>'.
	 * @generated
	 */
	InputDataModule createInputDataModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Base_column_structured_dataPackage getBase_column_structured_dataPackage();

} //Base_column_structured_dataFactory
