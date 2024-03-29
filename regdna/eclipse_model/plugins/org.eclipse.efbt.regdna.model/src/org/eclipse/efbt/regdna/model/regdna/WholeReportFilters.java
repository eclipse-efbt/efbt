/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Whole Report Filters</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.WholeReportFilters#getFilters <em>Filters</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getWholeReportFilters()
 * @model
 * @generated
 */
public interface WholeReportFilters extends EObject {
	/**
	 * Returns the value of the '<em><b>Filters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.Filter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Filters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getWholeReportFilters_Filters()
	 * @model containment="true"
	 * @generated
	 */
	EList<Filter> getFilters();

} // WholeReportFilters
