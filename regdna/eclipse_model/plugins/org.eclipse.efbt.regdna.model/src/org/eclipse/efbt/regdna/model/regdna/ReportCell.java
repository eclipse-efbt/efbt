/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Cell</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getRow <em>Row</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getFilters <em>Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getMetric <em>Metric</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getDatapointID <em>Datapoint ID</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell()
 * @model
 * @generated
 */
public interface ReportCell extends EObject {
	/**
	 * Returns the value of the '<em><b>Row</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Row</em>' reference.
	 * @see #setRow(ReportRow)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell_Row()
	 * @model
	 * @generated
	 */
	ReportRow getRow();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getRow <em>Row</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row</em>' reference.
	 * @see #getRow()
	 * @generated
	 */
	void setRow(ReportRow value);

	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(ReportColumn)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell_Column()
	 * @model
	 * @generated
	 */
	ReportColumn getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getColumn <em>Column</em>}' reference.
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
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell_Filters()
	 * @model containment="true"
	 * @generated
	 */
	EList<Filter> getFilters();

	/**
	 * Returns the value of the '<em><b>Metric</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Metric</em>' reference.
	 * @see #setMetric(ELAttribute)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell_Metric()
	 * @model
	 * @generated
	 */
	ELAttribute getMetric();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getMetric <em>Metric</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Metric</em>' reference.
	 * @see #getMetric()
	 * @generated
	 */
	void setMetric(ELAttribute value);

	/**
	 * Returns the value of the '<em><b>Datapoint ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Datapoint ID</em>' attribute.
	 * @see #setDatapointID(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getReportCell_DatapointID()
	 * @model
	 * @generated
	 */
	String getDatapointID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getDatapointID <em>Datapoint ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Datapoint ID</em>' attribute.
	 * @see #getDatapointID()
	 * @generated
	 */
	void setDatapointID(String value);

} // ReportCell
