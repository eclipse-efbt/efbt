/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Rules For Report</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport#getOutputLayerCube <em>Output Layer Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport#getRulesForTable <em>Rules For Table</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRulesForReport()
 * @model
 * @generated
 */
public interface RulesForReport extends EObject {
	/**
	 * Returns the value of the '<em><b>Output Layer Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Layer Cube</em>' reference.
	 * @see #setOutputLayerCube(ELClass)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRulesForReport_OutputLayerCube()
	 * @model
	 * @generated
	 */
	ELClass getOutputLayerCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport#getOutputLayerCube <em>Output Layer Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Layer Cube</em>' reference.
	 * @see #getOutputLayerCube()
	 * @generated
	 */
	void setOutputLayerCube(ELClass value);

	/**
	 * Returns the value of the '<em><b>Rules For Table</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.RulesForILTable}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rules For Table</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRulesForReport_RulesForTable()
	 * @model containment="true"
	 * @generated
	 */
	EList<RulesForILTable> getRulesForTable();

} // RulesForReport
