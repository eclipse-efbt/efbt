/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage
 * @generated
 */
public interface Column_structured_dataFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Column_structured_dataFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell</em>'.
	 * @generated
	 */
	Cell createCell();

	/**
	 * Returns a new object of class '<em>Column Structured Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Structured Data</em>'.
	 * @generated
	 */
	ColumnStructuredData createColumnStructuredData();

	/**
	 * Returns a new object of class '<em>Column Structured Data Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Structured Data Module</em>'.
	 * @generated
	 */
	ColumnStructuredDataModule createColumnStructuredDataModule();

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
	Column_structured_dataPackage getColumn_structured_dataPackage();

} //Column_structured_dataFactory
