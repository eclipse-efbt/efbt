/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.task.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.task.DataProcessingTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.task.impl.DataProcessingTaskImpl#getTargetAttribute <em>Target Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.task.impl.DataProcessingTaskImpl#getRequiredAtrributes <em>Required Atrributes</em>}</li>
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
	protected CUBE_STRUCTURE_ITEM targetAttribute;
	/**
	 * The cached value of the '{@link #getRequiredAtrributes() <em>Required Atrributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequiredAtrributes()
	 * @generated
	 * @ordered
	 */
	protected EList<CUBE_STRUCTURE_ITEM> requiredAtrributes;

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
	public CUBE_STRUCTURE_ITEM getTargetAttribute() {
		if (targetAttribute != null && targetAttribute.eIsProxy()) {
			InternalEObject oldTargetAttribute = (InternalEObject)targetAttribute;
			targetAttribute = (CUBE_STRUCTURE_ITEM)eResolveProxy(oldTargetAttribute);
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
	public CUBE_STRUCTURE_ITEM basicGetTargetAttribute() {
		return targetAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTargetAttribute(CUBE_STRUCTURE_ITEM newTargetAttribute) {
		CUBE_STRUCTURE_ITEM oldTargetAttribute = targetAttribute;
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
	public EList<CUBE_STRUCTURE_ITEM> getRequiredAtrributes() {
		if (requiredAtrributes == null) {
			requiredAtrributes = new EObjectResolvingEList<CUBE_STRUCTURE_ITEM>(CUBE_STRUCTURE_ITEM.class, this, TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES);
		}
		return requiredAtrributes;
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
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES:
				return getRequiredAtrributes();
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
				setTargetAttribute((CUBE_STRUCTURE_ITEM)newValue);
				return;
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES:
				getRequiredAtrributes().clear();
				getRequiredAtrributes().addAll((Collection<? extends CUBE_STRUCTURE_ITEM>)newValue);
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
				setTargetAttribute((CUBE_STRUCTURE_ITEM)null);
				return;
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES:
				getRequiredAtrributes().clear();
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
			case TaskPackage.DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES:
				return requiredAtrributes != null && !requiredAtrributes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DataProcessingTaskImpl
