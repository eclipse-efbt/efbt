/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.Scenario;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * The test definitions contained by the module
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition()
 * @model
 * @generated
 */
public interface TestDefinition extends EObject {
	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Any choice of sceanrios associated with this test definition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of  the test definition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // TestDefinition
