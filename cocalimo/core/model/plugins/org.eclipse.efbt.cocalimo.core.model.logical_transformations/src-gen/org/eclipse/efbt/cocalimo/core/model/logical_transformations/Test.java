/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getExpectedResult <em>Expected Result</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTest()
 * @model
 * @generated
 */
public interface Test extends EObject {
	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' reference.
	 * @see #setScenarios(Scenario)
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTest_Scenarios()
	 * @model
	 * @generated
	 */
	Scenario getScenarios();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getScenarios <em>Scenarios</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenarios</em>' reference.
	 * @see #getScenarios()
	 * @generated
	 */
	void setScenarios(Scenario value);

	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EObject}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Data</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTest_InputData()
	 * @model
	 * @generated
	 */
	EList<EObject> getInputData();

	/**
	 * Returns the value of the '<em><b>Expected Result</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EObject}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expected Result</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTest_ExpectedResult()
	 * @model
	 * @generated
	 */
	EList<EObject> getExpectedResult();

} // Test
