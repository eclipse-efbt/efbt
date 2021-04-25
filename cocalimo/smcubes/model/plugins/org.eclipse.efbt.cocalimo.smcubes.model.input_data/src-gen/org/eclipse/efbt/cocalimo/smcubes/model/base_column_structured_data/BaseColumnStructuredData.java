/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Column Structured Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Data For BaseCubes as a set of RowData
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getRows <em>Rows</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseColumnStructuredData()
 * @model
 * @generated
 */
public interface BaseColumnStructuredData extends ColumnStructuredData
{
	/**
	 * Returns the value of the '<em><b>Rows</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The rows which make up the table data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Rows</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseColumnStructuredData_Rows()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='rows'"
	 * @generated
	 */
	EList<BaseRowData> getRows();

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cube with which the data is associated
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseColumnStructuredData_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	CUBE getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(CUBE value);

} // BaseColumnStructuredData
