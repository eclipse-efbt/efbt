/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Useful Input Data Snippets</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A set of useful reusable input data
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets#getTestInputData <em>Test Input Data</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getUsefulInputDataSnippets()
 * @model
 * @generated
 */
public interface UsefulInputDataSnippets extends EObject
{
	/**
	 * Returns the value of the '<em><b>Test Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The reusable piece of TestData
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Input Data</em>' containment reference.
	 * @see #setTestInputData(TestInputData)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getUsefulInputDataSnippets_TestInputData()
	 * @model containment="true"
	 * @generated
	 */
	TestInputData getTestInputData();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets#getTestInputData <em>Test Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Input Data</em>' containment reference.
	 * @see #getTestInputData()
	 * @generated
	 */
	void setTestInputData(TestInputData value);

} // UsefulInputDataSnippets
