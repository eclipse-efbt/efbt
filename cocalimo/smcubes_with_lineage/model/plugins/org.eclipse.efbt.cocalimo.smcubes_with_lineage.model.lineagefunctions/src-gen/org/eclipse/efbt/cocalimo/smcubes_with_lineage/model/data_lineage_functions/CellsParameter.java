/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Parameter;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cells Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter which is an existing Cell. The Cell might be a BaseCell or a DerivedCell.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage#getCellsParameter()
 * @model
 * @generated
 */
public interface CellsParameter extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Cells</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cells</em>' reference.
	 * @see #setCells(Cell)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage#getCellsParameter_Cells()
	 * @model extendedMetaData="kind='attribute' name='cells'"
	 * @generated
	 */
	Cell getCells();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter#getCells <em>Cells</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cells</em>' reference.
	 * @see #getCells()
	 * @generated
	 */
	void setCells(Cell value);

} // CellsParameter
