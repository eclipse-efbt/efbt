/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension;

import org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cell Position Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of Cell Positions 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.CellPositionModule#getReportTables <em>Report Tables</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getCellPositionModule()
 * @model extendedMetaData="name='CellPositionModule' kind='elementOnly'"
 * @generated
 */
public interface CellPositionModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Report Tables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Cell Positions 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Tables</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getCellPositionModule_ReportTables()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportTables'"
	 * @generated
	 */
	EList<CELL_POSITION> getReportTables();

} // CellPositionModule
