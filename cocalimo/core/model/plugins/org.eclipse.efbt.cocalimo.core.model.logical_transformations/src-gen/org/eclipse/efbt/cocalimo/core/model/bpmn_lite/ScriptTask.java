/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Script Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask#getLinkedAttributes <em>Linked Attributes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getScriptTask()
 * @model
 * @generated
 */
public interface ScriptTask extends Task {
	/**
	 * Returns the value of the '<em><b>Linked Attributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Linked Attributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getScriptTask_LinkedAttributes()
	 * @model
	 * @generated
	 */
	EList<EAttribute> getLinkedAttributes();

} // ScriptTask
