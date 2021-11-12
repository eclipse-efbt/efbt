/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Set Visible Tasks For Scope</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.SetVisibleTasksForScope#getScriptTasksInScope <em>Script Tasks In Scope</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.SetVisibleTasksForScope#getScenariosOutOfScope <em>Scenarios Out Of Scope</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.SetVisibleTasksForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getSetVisibleTasksForScope()
 * @model
 * @generated
 */
public interface SetVisibleTasksForScope extends EObject {
	/**
	 * Returns the value of the '<em><b>Script Tasks In Scope</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Script Tasks In Scope</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getSetVisibleTasksForScope_ScriptTasksInScope()
	 * @model
	 * @generated
	 */
	EList<ScriptTask> getScriptTasksInScope();

	/**
	 * Returns the value of the '<em><b>Scenarios Out Of Scope</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios Out Of Scope</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getSetVisibleTasksForScope_ScenariosOutOfScope()
	 * @model
	 * @generated
	 */
	EList<Scenario> getScenariosOutOfScope();

	/**
	 * Returns the value of the '<em><b>Logical Transformation Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Logical Transformation Module</em>' reference.
	 * @see #setLogicalTransformationModule(LogicalTransformationModule)
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getSetVisibleTasksForScope_LogicalTransformationModule()
	 * @model
	 * @generated
	 */
	LogicalTransformationModule getLogicalTransformationModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.SetVisibleTasksForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Logical Transformation Module</em>' reference.
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	void setLogicalTransformationModule(LogicalTransformationModule value);

} // SetVisibleTasksForScope
