/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Results</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults#getReportResults <em>Report Results</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getReportResults()
 * @model
 * @generated
 */
public interface ReportResults extends EObject {
	/**
	 * Returns the value of the '<em><b>Report Results</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report Results</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getReportResults_ReportResults()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportCellValue> getReportResults();

} // ReportResults
