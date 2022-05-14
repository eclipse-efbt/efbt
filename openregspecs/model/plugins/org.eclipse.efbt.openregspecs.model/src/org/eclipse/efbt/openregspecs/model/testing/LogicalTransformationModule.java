/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getScenarioTags <em>Scenario Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSubProcess <em>Sub Process</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTestModules <em>Test Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSelectionLayers <em>Selection Layers</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule()
 * @model
 * @generated
 */
public interface LogicalTransformationModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Task Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.ActivityTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Task Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_TaskTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ActivityTag> getTaskTags();

	/**
	 * Returns the value of the '<em><b>Scenario Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.ScenarioTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_ScenarioTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioTag> getScenarioTags();

	/**
	 * Returns the value of the '<em><b>Sub Process</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Process</em>' containment reference.
	 * @see #setSubProcess(SubProcess)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_SubProcess()
	 * @model containment="true"
	 * @generated
	 */
	SubProcess getSubProcess();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSubProcess <em>Sub Process</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sub Process</em>' containment reference.
	 * @see #getSubProcess()
	 * @generated
	 */
	void setSubProcess(SubProcess value);

	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Requirements</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_Requirements()
	 * @model containment="true"
	 * @generated
	 */
	EList<RequirementsModule> getRequirements();

	/**
	 * Returns the value of the '<em><b>Test Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.TestModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Modules</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_TestModules()
	 * @model
	 * @generated
	 */
	EList<TestModule> getTestModules();

	/**
	 * Returns the value of the '<em><b>Selection Layers</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.SelectionLayer}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Selection Layers</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getLogicalTransformationModule_SelectionLayers()
	 * @model containment="true"
	 * @generated
	 */
	EList<SelectionLayer> getSelectionLayers();

} // LogicalTransformationModule
