/**
 */
package org.eclipse.efbt.cocamo.core.model.test_definition;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A TestDefinition decribes a parameterised test. It references a TestConstraint that it meets, which in turn references a TestTemplate that it meets to define the parameters that must be set and their expected types provided by example.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getTestContraints <em>Test Contraints</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getGiven <em>Given</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getWhen <em>When</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getThen <em>Then</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition()
 * @model
 * @generated
 */
public interface TestDefinition extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the TestDefinition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Test Contraints</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The constraints met by the TestDefinition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Contraints</em>' reference.
	 * @see #setTestContraints(TestContraints)
	 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition_TestContraints()
	 * @model
	 * @generated
	 */
	TestContraints getTestContraints();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getTestContraints <em>Test Contraints</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Contraints</em>' reference.
	 * @see #getTestContraints()
	 * @generated
	 */
	void setTestContraints(TestContraints value);

	/**
	 * Returns the value of the '<em><b>Given</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The given clause of the test
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Given</em>' containment reference.
	 * @see #setGiven(Given)
	 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition_Given()
	 * @model containment="true"
	 * @generated
	 */
	Given getGiven();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getGiven <em>Given</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Given</em>' containment reference.
	 * @see #getGiven()
	 * @generated
	 */
	void setGiven(Given value);

	/**
	 * Returns the value of the '<em><b>When</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The when clause of the test
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>When</em>' containment reference.
	 * @see #setWhen(When)
	 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition_When()
	 * @model containment="true"
	 * @generated
	 */
	When getWhen();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getWhen <em>When</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>When</em>' containment reference.
	 * @see #getWhen()
	 * @generated
	 */
	void setWhen(When value);

	/**
	 * Returns the value of the '<em><b>Then</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The then clause of the test
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Then</em>' containment reference.
	 * @see #setThen(Then)
	 * @see org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage#getTestDefinition_Then()
	 * @model containment="true"
	 * @generated
	 */
	Then getThen();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinition#getThen <em>Then</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Then</em>' containment reference.
	 * @see #getThen()
	 * @generated
	 */
	void setThen(Then value);

} // TestDefinition
