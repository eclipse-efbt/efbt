/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.task;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.emf.common.util.EList;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Data Processing Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A DataProcessingTask is a Task related to processing of data.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.task.DataProcessingTask#getTargetAttribute <em>Target Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.task.DataProcessingTask#getRequiredAtrributes <em>Required Atrributes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage#getDataProcessingTask()
 * @model
 * @generated
 */
public interface DataProcessingTask extends LeafTask {

	/**
	 * Returns the value of the '<em><b>Target Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Attribute</em>' reference.
	 * @see #setTargetAttribute(CUBE_STRUCTURE_ITEM)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage#getDataProcessingTask_TargetAttribute()
	 * @model
	 * @generated
	 */
	CUBE_STRUCTURE_ITEM getTargetAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.task.DataProcessingTask#getTargetAttribute <em>Target Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Attribute</em>' reference.
	 * @see #getTargetAttribute()
	 * @generated
	 */
	void setTargetAttribute(CUBE_STRUCTURE_ITEM value);

	/**
	 * Returns the value of the '<em><b>Required Atrributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Required Atrributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage#getDataProcessingTask_RequiredAtrributes()
	 * @model
	 * @generated
	 */
	EList<CUBE_STRUCTURE_ITEM> getRequiredAtrributes();
} // DataProcessingTask
