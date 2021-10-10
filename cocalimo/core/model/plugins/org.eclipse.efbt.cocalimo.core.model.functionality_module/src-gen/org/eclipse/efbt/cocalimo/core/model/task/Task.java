/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Task is the super class of ReportCellTask,ReadDataSourceTask, and DataSetTransformationTask.
 * It represents a set of processing.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenarioSet <em>Scenario Set</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.Task#isUsedInSubset <em>Used In Subset</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTask()
 * @model abstract="true"
 *        extendedMetaData="name='Task' kind='empty'"
 * @generated
 */
public interface Task extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the Task
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTask_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A Particular Scenario if the Task is assocuiated with a particular Scenario.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTask_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

	/**
	 * Returns the value of the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Any ScenarioSets associated with the  Task. 
	 * The individual Scenarios in such a ScenarioSet are associated with different behaviour in the Task that we would like to test seperately. 
	 * Scenarios can themsleves be linked to parts of the regulatory rulebook, allowing an annotation of the behaviour we wish to test.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario Set</em>' containment reference.
	 * @see #setScenarioSet(ScenarioSet)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTask_ScenarioSet()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='scenarioSet'"
	 * @generated
	 */
	ScenarioSet getScenarioSet();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenarioSet <em>Scenario Set</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario Set</em>' containment reference.
	 * @see #getScenarioSet()
	 * @generated
	 */
	void setScenarioSet(ScenarioSet value);

	/**
	 * Returns the value of the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Used In Subset</em>' attribute.
	 * @see #setUsedInSubset(boolean)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTask_UsedInSubset()
	 * @model
	 * @generated
	 */
	boolean isUsedInSubset();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#isUsedInSubset <em>Used In Subset</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Used In Subset</em>' attribute.
	 * @see #isUsedInSubset()
	 * @generated
	 */
	void setUsedInSubset(boolean value);

} // Task
