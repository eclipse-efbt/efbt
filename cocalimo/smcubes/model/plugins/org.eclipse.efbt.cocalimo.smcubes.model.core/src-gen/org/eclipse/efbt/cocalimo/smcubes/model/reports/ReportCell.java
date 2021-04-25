/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Cell</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Report Cell
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell#getCombination <em>Combination</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getReportCell()
 * @model extendedMetaData="name='ReportCell' kind='empty'"
 * @generated
 */
public interface ReportCell extends EObject
{
	/**
	 * Returns the value of the '<em><b>Combination</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The related Combination
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Combination</em>' reference.
	 * @see #setCombination(COMBINATION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getReportCell_Combination()
	 * @model extendedMetaData="kind='attribute' name='combination'"
	 * @generated
	 */
	COMBINATION getCombination();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell#getCombination <em>Combination</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Combination</em>' reference.
	 * @see #getCombination()
	 * @generated
	 */
	void setCombination(COMBINATION value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getReportCell_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // ReportCell
