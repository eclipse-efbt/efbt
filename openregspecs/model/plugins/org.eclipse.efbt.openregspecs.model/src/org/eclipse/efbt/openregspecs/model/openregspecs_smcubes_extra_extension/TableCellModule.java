/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension;

import org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Table Cell Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of TableCells 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.TableCellModule#getTableCells <em>Table Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getTableCellModule()
 * @model extendedMetaData="name='TableCellModule' kind='elementOnly'"
 * @generated
 */
public interface TableCellModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Table Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of TableCells 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Table Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getTableCellModule_TableCells()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='tableCells'"
	 * @generated
	 */
	EList<TABLE_CELL> getTableCells();

} // TableCellModule
