/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario Set Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A TaskSet whereby it is possible that ony some of the sub Transformation Schemes are executed for a test, dependant upon the Scenarios that are associated with the Test.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.ScenarioSetTask#getScenarioSetChoice <em>Scenario Set Choice</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getScenarioSetTask()
 * @model extendedMetaData="name='ScenarioSetTask' kind='elementOnly'"
 * @generated
 */
public interface ScenarioSetTask extends TaskSet {
	/**
	 * Returns the value of the '<em><b>Scenario Set Choice</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated  ScenarioSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario Set Choice</em>' containment reference.
	 * @see #setScenarioSetChoice(ScenarioSet)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getScenarioSetTask_ScenarioSetChoice()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='scenarioSet'"
	 * @generated
	 */
	ScenarioSet getScenarioSetChoice();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.ScenarioSetTask#getScenarioSetChoice <em>Scenario Set Choice</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario Set Choice</em>' containment reference.
	 * @see #getScenarioSetChoice()
	 * @generated
	 */
	void setScenarioSetChoice(ScenarioSet value);

} // ScenarioSetTask
