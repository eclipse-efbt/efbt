/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.scenarios;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario Set Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Tag which tags a ScenarioSet in order to link it with requirements.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSetTag#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenariosPackage#getScenarioSetTag()
 * @model
 * @generated
 */
public interface ScenarioSetTag extends Tag {
	/**
	 * Returns the value of the '<em><b>Scenario Set</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The tagged ScenarioSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario Set</em>' reference.
	 * @see #setScenarioSet(ScenarioSet)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenariosPackage#getScenarioSetTag_ScenarioSet()
	 * @model
	 * @generated
	 */
	ScenarioSet getScenarioSet();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSetTag#getScenarioSet <em>Scenario Set</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario Set</em>' reference.
	 * @see #getScenarioSet()
	 * @generated
	 */
	void setScenarioSet(ScenarioSet value);

} // ScenarioSetTag
