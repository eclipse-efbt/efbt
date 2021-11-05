/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Populated Report</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Populated Report, described as a set of ReportCellValues. It is not required that there be a ReportCellValues 
 *       are all linked to one Table from the rendering package
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport#getReportCellValues <em>Report Cell Values</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getPopulatedReport()
 * @model extendedMetaData="name='PopulatedReport' kind='elementOnly'"
 * @generated
 */
public interface PopulatedReport extends EObject {
	/**
	 * Returns the value of the '<em><b>Report Cell Values</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ReportCellValues
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cell Values</em>' containment reference.
	 * @see #setReportCellValues(ReportCellValue)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getPopulatedReport_ReportCellValues()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportCellValues'"
	 * @generated
	 */
	ReportCellValue getReportCellValues();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport#getReportCellValues <em>Report Cell Values</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Report Cell Values</em>' containment reference.
	 * @see #getReportCellValues()
	 * @generated
	 */
	void setReportCellValues(ReportCellValue value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the PopulatedReport
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getPopulatedReport_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // PopulatedReport
