/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Data For Rows of cubes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getRowID <em>Row ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getRowData()
 * @model
 * @generated
 */
public interface RowData extends EObject {
	/**
	 * Returns the value of the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The unique ID of the row data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Row ID</em>' attribute.
	 * @see #setRowID(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getRowData_RowID()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='rowID'"
	 * @generated
	 */
	String getRowID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getRowID <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row ID</em>' attribute.
	 * @see #getRowID()
	 * @generated
	 */
	void setRowID(String value);

	/**
	 * Returns the value of the '<em><b>Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cells containing the data associated with the Rows
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getRowData_Cells()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cells'"
	 * @generated
	 */
	EList<Cell> getCells();

} // RowData
