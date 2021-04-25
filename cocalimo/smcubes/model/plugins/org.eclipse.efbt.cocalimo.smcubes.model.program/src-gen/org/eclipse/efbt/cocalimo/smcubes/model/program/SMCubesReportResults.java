/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program;

import org.eclipse.efbt.cocalimo.core.model.test.Result;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Report Results</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Expected results which follwo the way that SMCubes represents Report Celss (As COMBINATIONs)
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults#getReportCells <em>Report Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesReportResults()
 * @model
 * @generated
 */
public interface SMCubesReportResults extends Result {
	/**
	 * Returns the value of the '<em><b>Report Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The value of a ReportCell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesReportResults_ReportCells()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportCellValue> getReportCells();

} // SMCubesReportResults
