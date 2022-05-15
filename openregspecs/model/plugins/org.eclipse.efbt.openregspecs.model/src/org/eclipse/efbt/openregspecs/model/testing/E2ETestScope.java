/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.ScriptTask;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>E2E Test Scope</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getLayer <em>Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScriptTask <em>Script Task</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getE2ETestScope()
 * @model
 * @generated
 */
public interface E2ETestScope extends TestScope {
	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getE2ETestScope_Scenarios()
	 * @model
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Layer</em>' reference.
	 * @see #setLayer(SelectionLayer)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getE2ETestScope_Layer()
	 * @model
	 * @generated
	 */
	SelectionLayer getLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getLayer <em>Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Layer</em>' reference.
	 * @see #getLayer()
	 * @generated
	 */
	void setLayer(SelectionLayer value);

	/**
	 * Returns the value of the '<em><b>Script Task</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Script Task</em>' reference.
	 * @see #setScriptTask(ScriptTask)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getE2ETestScope_ScriptTask()
	 * @model
	 * @generated
	 */
	ScriptTask getScriptTask();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScriptTask <em>Script Task</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Script Task</em>' reference.
	 * @see #getScriptTask()
	 * @generated
	 */
	void setScriptTask(ScriptTask value);

} // E2ETestScope
