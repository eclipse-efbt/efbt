/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getReportResults <em>Report Results</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getScenario <em>Scenario</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTest()
 * @model
 * @generated
 */
public interface Test extends EObject {
	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Data</em>' reference.
	 * @see #setInputData(InputData)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTest_InputData()
	 * @model
	 * @generated
	 */
	InputData getInputData();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getInputData <em>Input Data</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Input Data</em>' reference.
	 * @see #getInputData()
	 * @generated
	 */
	void setInputData(InputData value);

	/**
	 * Returns the value of the '<em><b>Report Results</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report Results</em>' reference.
	 * @see #setReportResults(ReportResults)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTest_ReportResults()
	 * @model
	 * @generated
	 */
	ReportResults getReportResults();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getReportResults <em>Report Results</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Report Results</em>' reference.
	 * @see #getReportResults()
	 * @generated
	 */
	void setReportResults(ReportResults value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTest_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTest_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

} // Test
