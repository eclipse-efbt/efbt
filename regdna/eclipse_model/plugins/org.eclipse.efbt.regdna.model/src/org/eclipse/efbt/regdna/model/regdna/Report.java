/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Report#getOutputLayer <em>Output Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Report#getRows <em>Rows</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Report#getColumns <em>Columns</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReport()
 * @model
 * @generated
 */
public interface Report extends EObject {
	/**
	 * Returns the value of the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Layer</em>' reference.
	 * @see #setOutputLayer(ELClass)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReport_OutputLayer()
	 * @model
	 * @generated
	 */
	ELClass getOutputLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Report#getOutputLayer <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Layer</em>' reference.
	 * @see #getOutputLayer()
	 * @generated
	 */
	void setOutputLayer(ELClass value);

	/**
	 * Returns the value of the '<em><b>Rows</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ReportRow}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rows</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReport_Rows()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportRow> getRows();

	/**
	 * Returns the value of the '<em><b>Columns</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ReportColumn}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Columns</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReport_Columns()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportColumn> getColumns();

} // Report
