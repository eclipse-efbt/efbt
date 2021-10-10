/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Read Data Source Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Task which represents the reading in of data.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask#getEntity <em>Entity</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getReadDataSourceTask()
 * @model extendedMetaData="name='ReadDataSourceTask' kind='elementOnly'"
 * @generated
 */
public interface ReadDataSourceTask extends LeafTask {
	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(EClass)
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getReadDataSourceTask_Entity()
	 * @model
	 * @generated
	 */
	EClass getEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(EClass value);

} // ReadDataSourceTask
