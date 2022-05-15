/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.impl;

import org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET;

import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK;
import org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK_VARIABLE_SET;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>FRAMEWORK VARIABLE SET</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FRAMEWORK_VARIABLE_SETImpl extends MinimalEObjectImpl.Container implements FRAMEWORK_VARIABLE_SET {
	/**
	 * The cached value of the '{@link #getFramework_id() <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFramework_id()
	 * @generated
	 * @ordered
	 */
	protected FRAMEWORK framework_id;

	/**
	 * The cached value of the '{@link #getVariable_set_id() <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_set_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE_SET variable_set_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FRAMEWORK_VARIABLE_SETImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.FRAMEWORK_VARIABLE_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FRAMEWORK getFramework_id() {
		if (framework_id != null && framework_id.eIsProxy()) {
			InternalEObject oldFramework_id = (InternalEObject)framework_id;
			framework_id = (FRAMEWORK)eResolveProxy(oldFramework_id);
			if (framework_id != oldFramework_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID, oldFramework_id, framework_id));
			}
		}
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FRAMEWORK basicGetFramework_id() {
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFramework_id(FRAMEWORK newFramework_id) {
		FRAMEWORK oldFramework_id = framework_id;
		framework_id = newFramework_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID, oldFramework_id, framework_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET getVariable_set_id() {
		if (variable_set_id != null && variable_set_id.eIsProxy()) {
			InternalEObject oldVariable_set_id = (InternalEObject)variable_set_id;
			variable_set_id = (VARIABLE_SET)eResolveProxy(oldVariable_set_id);
			if (variable_set_id != oldVariable_set_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
			}
		}
		return variable_set_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET basicGetVariable_set_id() {
		return variable_set_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVariable_set_id(VARIABLE_SET newVariable_set_id) {
		VARIABLE_SET oldVariable_set_id = variable_set_id;
		variable_set_id = newVariable_set_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID:
				if (resolve) return getFramework_id();
				return basicGetFramework_id();
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID:
				if (resolve) return getVariable_set_id();
				return basicGetVariable_set_id();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)newValue);
				return;
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)newValue);
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
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)null);
				return;
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)null);
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
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID:
				return framework_id != null;
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID:
				return variable_set_id != null;
		}
		return super.eIsSet(featureID);
	}

} //FRAMEWORK_VARIABLE_SETImpl
