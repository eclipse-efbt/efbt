/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getExpectedResults <em>Expected Results</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getTestDefinition <em>Test Definition</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTest()
 * @model
 * @generated
 */
public interface Test extends EObject {
	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The input data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Input Data</em>' containment reference.
	 * @see #setInputData(InputData)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTest_InputData()
	 * @model containment="true"
	 * @generated
	 */
	InputData getInputData();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getInputData <em>Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Input Data</em>' containment reference.
	 * @see #getInputData()
	 * @generated
	 */
	void setInputData(InputData value);

	/**
	 * Returns the value of the '<em><b>Expected Results</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The expected results
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Expected Results</em>' containment reference.
	 * @see #setExpectedResults(ReportResults)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTest_ExpectedResults()
	 * @model containment="true"
	 * @generated
	 */
	ReportResults getExpectedResults();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getExpectedResults <em>Expected Results</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Expected Results</em>' containment reference.
	 * @see #getExpectedResults()
	 * @generated
	 */
	void setExpectedResults(ReportResults value);

	/**
	 * Returns the value of the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Definition</em>' reference.
	 * @see #setTestDefinition(TestDefinition)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTest_TestDefinition()
	 * @model
	 * @generated
	 */
	TestDefinition getTestDefinition();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getTestDefinition <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Definition</em>' reference.
	 * @see #getTestDefinition()
	 * @generated
	 */
	void setTestDefinition(TestDefinition value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTest_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // Test
