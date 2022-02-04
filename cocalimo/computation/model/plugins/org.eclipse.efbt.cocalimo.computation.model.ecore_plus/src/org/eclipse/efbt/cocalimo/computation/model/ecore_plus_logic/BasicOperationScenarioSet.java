/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Operation Scenario Set</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet#getBasicScenarioLogic <em>Basic Scenario Logic</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicOperationScenarioSet()
 * @model
 * @generated
 */
public interface BasicOperationScenarioSet extends Operation {
	/**
	 * Returns the value of the '<em><b>Basic Scenario Logic</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Basic Scenario Logic</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicOperationScenarioSet_BasicScenarioLogic()
	 * @model containment="true"
	 * @generated
	 */
	EList<BasicScenarioOperation> getBasicScenarioLogic();

} // BasicOperationScenarioSet
