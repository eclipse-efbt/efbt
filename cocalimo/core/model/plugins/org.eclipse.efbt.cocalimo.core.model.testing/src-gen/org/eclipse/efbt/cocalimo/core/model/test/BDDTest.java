/**
 */
package org.eclipse.efbt.cocalimo.core.model.test;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>BDD Test</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An end to end test
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test.BDDTest#getTestDefinition <em>Test Definition</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test.TestPackage#getBDDTest()
 * @model
 * @generated
 */
public interface BDDTest extends Test
{
	/**
	 * Returns the value of the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TesDefinition associated with the test
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Definition</em>' reference.
	 * @see #setTestDefinition(BDDTestDefinition)
	 * @see org.eclipse.efbt.cocalimo.core.model.test.TestPackage#getBDDTest_TestDefinition()
	 * @model
	 * @generated
	 */
	BDDTestDefinition getTestDefinition();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test.BDDTest#getTestDefinition <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Definition</em>' reference.
	 * @see #getTestDefinition()
	 * @generated
	 */
	void setTestDefinition(BDDTestDefinition value);

} // BDDTest
