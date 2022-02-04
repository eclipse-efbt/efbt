/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Scenario Operation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getSceanrio <em>Sceanrio</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioOperation()
 * @model
 * @generated
 */
public interface BasicScenarioOperation extends Operation {
	/**
	 * Returns the value of the '<em><b>Sceanrio</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sceanrio</em>' reference.
	 * @see #setSceanrio(Scenario)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioOperation_Sceanrio()
	 * @model
	 * @generated
	 */
	Scenario getSceanrio();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getSceanrio <em>Sceanrio</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sceanrio</em>' reference.
	 * @see #getSceanrio()
	 * @generated
	 */
	void setSceanrio(Scenario value);

	/**
	 * Returns the value of the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Function Spec</em>' reference.
	 * @see #setFunctionSpec(BasicFunctionSpec)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioOperation_FunctionSpec()
	 * @model
	 * @generated
	 */
	BasicFunctionSpec getFunctionSpec();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getFunctionSpec <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec</em>' reference.
	 * @see #getFunctionSpec()
	 * @generated
	 */
	void setFunctionSpec(BasicFunctionSpec value);

} // BasicScenarioOperation
