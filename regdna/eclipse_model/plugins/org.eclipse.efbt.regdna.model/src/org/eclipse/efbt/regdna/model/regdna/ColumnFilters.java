/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Column Filters</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters#getFilters <em>Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters#getMetric <em>Metric</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getColumnFilters()
 * @model
 * @generated
 */
public interface ColumnFilters extends EObject {
	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(ReportColumn)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getColumnFilters_Column()
	 * @model
	 * @generated
	 */
	ReportColumn getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(ReportColumn value);

	/**
	 * Returns the value of the '<em><b>Filters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.Filter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Filters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getColumnFilters_Filters()
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
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getColumnFilters_Metric()
	 * @model
	 * @generated
	 */
	ELOperation getMetric();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ColumnFilters#getMetric <em>Metric</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Metric</em>' reference.
	 * @see #getMetric()
	 * @generated
	 */
	void setMetric(ELOperation value);

} // ColumnFilters
