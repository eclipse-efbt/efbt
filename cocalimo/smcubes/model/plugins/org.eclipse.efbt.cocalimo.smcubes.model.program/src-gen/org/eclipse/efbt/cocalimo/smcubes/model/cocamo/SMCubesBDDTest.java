/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo;

import org.eclipse.efbt.cocalimo.core.model.test.BDDTest;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes BDD Test</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A test specific to the SMCubes methodolgy of describing Input Data structures and Output DataStructures (As cubes of typically COMBINATIONS representing report cells)
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest#getExpectedResults <em>Expected Results</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesBDDTest()
 * @model
 * @generated
 */
public interface SMCubesBDDTest extends BDDTest
{
	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The input data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Input Data</em>' containment reference.
	 * @see #setInputData(SMCubesTestInputData)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesBDDTest_InputData()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesTestInputData getInputData();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest#getInputData <em>Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Input Data</em>' containment reference.
	 * @see #getInputData()
	 * @generated
	 */
	void setInputData(SMCubesTestInputData value);

	/**
	 * Returns the value of the '<em><b>Expected Results</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The expected results
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Expected Results</em>' reference.
	 * @see #setExpectedResults(SMCubesReportResults)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesBDDTest_ExpectedResults()
	 * @model
	 * @generated
	 */
	SMCubesReportResults getExpectedResults();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest#getExpectedResults <em>Expected Results</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Expected Results</em>' reference.
	 * @see #getExpectedResults()
	 * @generated
	 */
	void setExpectedResults(SMCubesReportResults value);

} // SMCubesBDDTest
