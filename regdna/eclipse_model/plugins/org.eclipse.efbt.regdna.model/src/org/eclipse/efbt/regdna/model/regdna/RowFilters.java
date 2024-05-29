/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Filters</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowFilters#getRow <em>Row</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowFilters#getFilters <em>Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RowFilters#getMetric <em>Metric</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowFilters()
 * @model
 * @generated
 */
public interface RowFilters extends EObject {
	/**
	 * Returns the value of the '<em><b>Row</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Row</em>' reference.
	 * @see #setRow(ReportRow)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowFilters_Row()
	 * @model
	 * @generated
	 */
	ReportRow getRow();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RowFilters#getRow <em>Row</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row</em>' reference.
	 * @see #getRow()
	 * @generated
	 */
	void setRow(ReportRow value);

	/**
	 * Returns the value of the '<em><b>Filters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.Filter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Filters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowFilters_Filters()
	 * @model containment="true"
	 * @generated
	 */
	EList<Filter> getFilters();

	/**
	 * Returns the value of the '<em><b>Metric</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Metric</em>' reference.
	 * @see #setMetric(ELOperation)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRowFilters_Metric()
	 * @model
	 * @generated
	 */
	ELOperation getMetric();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RowFilters#getMetric <em>Metric</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Metric</em>' reference.
	 * @see #getMetric()
	 * @generated
	 */
	void setMetric(ELOperation value);

} // RowFilters
