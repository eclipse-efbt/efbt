/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.Test#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.Test#getExpectedResult <em>Expected Result</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.Test#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.Test#getScope <em>Scope</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTest()
 * @model
 * @generated
 */
public interface Test extends EObject {
	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.CSVFile}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Data</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTest_InputData()
	 * @model
	 * @generated
	 */
	EList<CSVFile> getInputData();

	/**
	 * Returns the value of the '<em><b>Expected Result</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.CSVFile}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expected Result</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTest_ExpectedResult()
	 * @model
	 * @generated
	 */
	EList<CSVFile> getExpectedResult();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTest_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Scope</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scope</em>' containment reference.
	 * @see #setScope(TestScope)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTest_Scope()
	 * @model containment="true"
	 * @generated
	 */
	TestScope getScope();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getScope <em>Scope</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scope</em>' containment reference.
	 * @see #getScope()
	 * @generated
	 */
	void setScope(TestScope value);

} // Test
