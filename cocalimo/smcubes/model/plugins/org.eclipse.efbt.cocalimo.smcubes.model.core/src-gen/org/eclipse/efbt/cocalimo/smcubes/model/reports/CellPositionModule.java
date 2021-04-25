/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.CellPositionModule#getReportTables <em>Report Tables</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getCellPositionModule()
 * @model extendedMetaData="name='CellPositionModule' kind='elementOnly'"
 * @generated
 */
public interface CellPositionModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Report Tables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Cell Positions 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Tables</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getCellPositionModule_ReportTables()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportTables'"
	 * @generated
	 */
	EList<CELL_POSITION> getReportTables();

} // CellPositionModule
