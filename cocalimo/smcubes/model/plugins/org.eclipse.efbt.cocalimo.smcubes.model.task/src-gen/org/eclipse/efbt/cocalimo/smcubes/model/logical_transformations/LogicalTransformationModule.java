/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSetTag;
import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioTag;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Logical Transformation Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule#getScenarioTags <em>Scenario Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule#getScenarioSetTags <em>Scenario Set Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule#getSubProcesses <em>Sub Processes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#getLogicalTransformationModule()
 * @model
 * @generated
 */
public interface LogicalTransformationModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Task Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.TaskTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Task Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#getLogicalTransformationModule_TaskTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<TaskTag> getTaskTags();

	/**
	 * Returns the value of the '<em><b>Scenario Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#getLogicalTransformationModule_ScenarioTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioTag> getScenarioTags();

	/**
	 * Returns the value of the '<em><b>Scenario Set Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSetTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario Set Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#getLogicalTransformationModule_ScenarioSetTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioSetTag> getScenarioSetTags();

	/**
	 * Returns the value of the '<em><b>Sub Processes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.SubProcess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Processes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#getLogicalTransformationModule_SubProcesses()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubProcess> getSubProcesses();

} // LogicalTransformationModule
