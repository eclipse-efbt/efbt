/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Column Based Report</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowColumnBasedReport#getColumnFilters <em>Column Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowColumnBasedReport#getRowFilters <em>Row Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowColumnBasedReport#getWholeReportFilters <em>Whole Report Filters</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowColumnBasedReport()
 * @model
 * @generated
 */
public interface RowColumnBasedReport extends Report {
	/**
	 * Returns the value of the '<em><b>Column Filters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column Filters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowColumnBasedReport_ColumnFilters()
	 * @model containment="true"
	 * @generated
	 */
	EList<ColumnFilters> getColumnFilters();

	/**
	 * Returns the value of the '<em><b>Row Filters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.RowFilters}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Row Filters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowColumnBasedReport_RowFilters()
	 * @model containment="true"
	 * @generated
	 */
	EList<RowFilters> getRowFilters();

	/**
	 * Returns the value of the '<em><b>Whole Report Filters</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Whole Report Filters</em>' containment reference.
	 * @see #setWholeReportFilters(WholeReportFilters)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowColumnBasedReport_WholeReportFilters()
	 * @model containment="true"
	 * @generated
	 */
	WholeReportFilters getWholeReportFilters();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RowColumnBasedReport#getWholeReportFilters <em>Whole Report Filters</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Whole Report Filters</em>' containment reference.
	 * @see #getWholeReportFilters()
	 * @generated
	 */
	void setWholeReportFilters(WholeReportFilters value);

} // RowColumnBasedReport
