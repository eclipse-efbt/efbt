/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Data For Rows of column structures such as cubes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData#getRowID <em>Row ID</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage#getRowData()
 * @model
 * @generated
 */
public interface RowData extends EObject
{
	/**
	 * Returns the value of the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The unique ID of the row data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Row ID</em>' attribute.
	 * @see #setRowID(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage#getRowData_RowID()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='rowID'"
	 * @generated
	 */
	String getRowID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData#getRowID <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row ID</em>' attribute.
	 * @see #getRowID()
	 * @generated
	 */
	void setRowID(String value);

} // RowData
