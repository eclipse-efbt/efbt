/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Row Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  A row of derived data 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getFormulaCells <em>Formula Cells</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSpecialDerivedCells <em>Special Derived Cells</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSourceRows <em>Source Rows</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedRowData()
 * @model extendedMetaData="name='DerivedRowData' kind='elementOnly'"
 * @generated
 */
public interface DerivedRowData extends RowData
{
	/**
	 * Returns the value of the '<em><b>Formula Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The derived cells
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Formula Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedRowData_FormulaCells()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='formulaCells'"
	 * @generated
	 */
	EList<FormulaCell> getFormulaCells();

	/**
	 * Returns the value of the '<em><b>Special Derived Cells</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The special DerivedCells, not created by functions which use cells from source rows.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Special Derived Cells</em>' containment reference.
	 * @see #setSpecialDerivedCells(FormulaCell)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedRowData_SpecialDerivedCells()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='specialDerivedCells'"
	 * @generated
	 */
	FormulaCell getSpecialDerivedCells();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSpecialDerivedCells <em>Special Derived Cells</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Special Derived Cells</em>' containment reference.
	 * @see #getSpecialDerivedCells()
	 * @generated
	 */
	void setSpecialDerivedCells(FormulaCell value);

	/**
	 * Returns the value of the '<em><b>Source Rows</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * source rows of the derived row
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Source Rows</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedRowData_SourceRows()
	 * @model extendedMetaData="kind='attribute' name='sourceRows'"
	 * @generated
	 */
	EList<RowData> getSourceRows();

} // DerivedRowData
