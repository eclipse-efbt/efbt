/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite;

import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.SelectionLayer;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Script Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.ScriptTask#getSelectionLayers <em>Selection Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.ScriptTask#getOutputLayer <em>Output Layer</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getScriptTask()
 * @model
 * @generated
 */
public interface ScriptTask extends Task {
	/**
	 * Returns the value of the '<em><b>Selection Layers</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.SelectionLayer}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Selection Layers</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getScriptTask_SelectionLayers()
	 * @model containment="true"
	 * @generated
	 */
	EList<SelectionLayer> getSelectionLayers();

	/**
	 * Returns the value of the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Layer</em>' reference.
	 * @see #setOutputLayer(EClass)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getScriptTask_OutputLayer()
	 * @model
	 * @generated
	 */
	EClass getOutputLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.ScriptTask#getOutputLayer <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Layer</em>' reference.
	 * @see #getOutputLayer()
	 * @generated
	 */
	void setOutputLayer(EClass value);

} // ScriptTask
