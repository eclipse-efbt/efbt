/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Scenario Logic</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getBasicFunction <em>Basic Function</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getSceanrio <em>Sceanrio</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioLogic()
 * @model
 * @generated
 */
public interface BasicScenarioLogic extends EObject {
	/**
	 * Returns the value of the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated function
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Basic Function</em>' containment reference.
	 * @see #setBasicFunction(BasicFunction)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioLogic_BasicFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='basicFunction'"
	 * @generated
	 */
	BasicFunction getBasicFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getBasicFunction <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Basic Function</em>' containment reference.
	 * @see #getBasicFunction()
	 * @generated
	 */
	void setBasicFunction(BasicFunction value);

	/**
	 * Returns the value of the '<em><b>Sceanrio</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sceanrio</em>' reference.
	 * @see #setSceanrio(Scenario)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicScenarioLogic_Sceanrio()
	 * @model
	 * @generated
	 */
	Scenario getSceanrio();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getSceanrio <em>Sceanrio</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sceanrio</em>' reference.
	 * @see #getSceanrio()
	 * @generated
	 */
	void setSceanrio(Scenario value);

} // BasicScenarioLogic
