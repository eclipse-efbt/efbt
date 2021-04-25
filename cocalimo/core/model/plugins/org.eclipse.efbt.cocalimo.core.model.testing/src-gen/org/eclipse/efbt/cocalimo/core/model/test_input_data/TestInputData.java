/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Input Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * InputData for a test
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getTestInputData()
 * @model extendedMetaData="name='InputData' kind='elementOnly'"
 * @generated
 */
public interface TestInputData extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name associated with the input data.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getTestInputData_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // TestInputData
