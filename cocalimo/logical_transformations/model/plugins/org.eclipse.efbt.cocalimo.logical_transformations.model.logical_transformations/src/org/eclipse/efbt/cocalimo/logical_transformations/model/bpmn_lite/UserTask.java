/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BasicEntity;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>User Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.UserTask#getEntity <em>Entity</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getUserTask()
 * @model
 * @generated
 */
public interface UserTask extends Task {
	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(BasicEntity)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getUserTask_Entity()
	 * @model
	 * @generated
	 */
	BasicEntity getEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.UserTask#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(BasicEntity value);

} // UserTask
