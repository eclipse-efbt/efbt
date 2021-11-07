/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cell</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Cell data of the Rows of populated cubes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getCellID <em>Cell ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getCell()
 * @model
 * @generated
 */
public interface Cell extends EObject {
	/**
	 * Returns the value of the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The unique ID of a cell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cell ID</em>' attribute.
	 * @see #setCellID(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getCell_CellID()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='cellID'"
	 * @generated
	 */
	String getCellID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getCellID <em>Cell ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cell ID</em>' attribute.
	 * @see #getCellID()
	 * @generated
	 */
	void setCellID(String value);

	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The variable associated with the cell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getCell_Column()
	 * @model extendedMetaData="kind='attribute' name='column'"
	 * @generated
	 */
	VARIABLE getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(VARIABLE value);

} // Cell
