/**
 */
package org.eclipse.efbt.cocalimo.core.model.test;

import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Reg Functionality Test</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Test
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest#getTestDefinition <em>Test Definition</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test.TestPackage#getRegFunctionalityTest()
 * @model
 * @generated
 */
public interface RegFunctionalityTest extends Test
{
	/**
	 * Returns the value of the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Definition</em>' reference.
	 * @see #setTestDefinition(RegFunctionalityTestDefinition)
	 * @see org.eclipse.efbt.cocalimo.core.model.test.TestPackage#getRegFunctionalityTest_TestDefinition()
	 * @model
	 * @generated
	 */
	RegFunctionalityTestDefinition getTestDefinition();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest#getTestDefinition <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Definition</em>' reference.
	 * @see #getTestDefinition()
	 * @generated
	 */
	void setTestDefinition(RegFunctionalityTestDefinition value);

} // RegFunctionalityTest
