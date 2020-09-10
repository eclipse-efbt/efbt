/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.RowData;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Row Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Data For Rows of BaseCubes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseRowData#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseRowData()
 * @model
 * @generated
 */
public interface BaseRowData extends RowData
{
	/**
	 * Returns the value of the '<em><b>Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCell}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cells containing the data associated with the BaseRows
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseRowData_Cells()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cells'"
	 * @generated
	 */
	EList<BaseCell> getCells();

} // BaseRowData
