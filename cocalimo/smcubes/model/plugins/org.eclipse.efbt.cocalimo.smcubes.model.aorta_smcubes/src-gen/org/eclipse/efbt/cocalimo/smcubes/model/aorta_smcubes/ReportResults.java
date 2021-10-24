/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Results</b></em>'.
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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults#getReportCells <em>Report Cells</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getReportResults()
 * @model
 * @generated
 */
public interface ReportResults extends EObject {
	/**
	 * Returns the value of the '<em><b>Report Cells</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The value of a ReportCell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cells</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getReportResults_ReportCells()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportCellValue> getReportCells();

} // ReportResults
