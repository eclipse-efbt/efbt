/**
 */
package org.eclipse.efbt.cocalimo.core.model.task.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask;
import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Data Processing Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl#getTargetAttribute <em>Target Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl#getRequiredAttributes <em>Required Attributes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DataProcessingTaskImpl extends LeafTaskImpl implements DataProcessingTask {
	/**
	 * The cached value of the '{@link #getTargetAttribute() <em>Target Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTargetAttribute()
	 * @generated
	 * @ordered
	 */
	protected org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM targetAttribute;
	/**
	 * The cached value of the '{@link #getRequiredAttributes() <em>Required Attributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequiredAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM> requiredAttributes;
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DataProcessingTaskImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TaskPackage.Literals.DATA_PROCESSING_TASK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM getTargetAttribute() {
		if (targetAttribute != null && targetAttribute.eIsProxy()) {
			InternalEObject oldTargetAttribute = (InternalEObject)targetAttribute;
			targetAttribute = (org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM)eResolveProxy(oldTargetAttribute);
			if (targetAttribute != oldTargetAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE, oldTargetAttribute, targetAttribute));
			}
		}
		return targetAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM basicGetTargetAttribute() {
		return targetAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTargetAttribute(org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM newTargetAttribute) {
		org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM oldTargetAttribute = targetAttribute;
		targetAttribute = newTargetAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE, oldTargetAttribute, targetAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM> getRequiredAttributes() {
		if (requiredAttributes == null) {
			requiredAttributes = new EObjectResolvingEList<org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM>(org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, this, TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATTRIBUTES);
		}
		return requiredAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE:
				if (resolve) return getTargetAttribute();
				return basicGetTargetAttribute();
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATTRIBUTES:
				return getRequiredAttributes();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE:
				setTargetAttribute((org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM)newValue);
				return;
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				getRequiredAttributes().addAll((Collection<? extends org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE:
				setTargetAttribute((org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM)null);
				return;
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case TaskPackage.DATA_PROCESSING_TASK__TARGET_ATTRIBUTE:
				return targetAttribute != null;
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATTRIBUTES:
				return requiredAttributes != null && !requiredAttributes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DataProcessingTaskImpl
