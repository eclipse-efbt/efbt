/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>COMBINATION ITEM</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class COMBINATION_ITEMImpl extends MinimalEObjectImpl.Container implements COMBINATION_ITEM {
	/**
	 * The cached value of the '{@link #getMember_id() <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_id()
	 * @generated
	 * @ordered
	 */
	protected MEMBER member_id;

	/**
	 * The cached value of the '{@link #getSubdomain_id() <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomain_id()
	 * @generated
	 * @ordered
	 */
	protected SUBDOMAIN subdomain_id;

	/**
	 * The cached value of the '{@link #getVariable_id() <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable_id;

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
	protected COMBINATION_ITEMImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.COMBINATION_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER getMember_id() {
		if (member_id != null && member_id.eIsProxy()) {
			InternalEObject oldMember_id = (InternalEObject)member_id;
			member_id = (MEMBER)eResolveProxy(oldMember_id);
			if (member_id != oldMember_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID, oldMember_id, member_id));
			}
		}
		return member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetMember_id() {
		return member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember_id(MEMBER newMember_id) {
		MEMBER oldMember_id = member_id;
		member_id = newMember_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID, oldMember_id, member_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN getSubdomain_id() {
		if (subdomain_id != null && subdomain_id.eIsProxy()) {
			InternalEObject oldSubdomain_id = (InternalEObject)subdomain_id;
			subdomain_id = (SUBDOMAIN)eResolveProxy(oldSubdomain_id);
			if (subdomain_id != oldSubdomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
			}
		}
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN basicGetSubdomain_id() {
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubdomain_id(SUBDOMAIN newSubdomain_id) {
		SUBDOMAIN oldSubdomain_id = subdomain_id;
		subdomain_id = newSubdomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getVariable_id() {
		if (variable_id != null && variable_id.eIsProxy()) {
			InternalEObject oldVariable_id = (InternalEObject)variable_id;
			variable_id = (VARIABLE)eResolveProxy(oldVariable_id);
			if (variable_id != oldVariable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
			}
		}
		return variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVariable_id() {
		return variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariable_id(VARIABLE newVariable_id) {
		VARIABLE oldVariable_id = variable_id;
		variable_id = newVariable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_SET getVariable_set_id() {
		if (variable_set_id != null && variable_set_id.eIsProxy()) {
			InternalEObject oldVariable_set_id = (InternalEObject)variable_set_id;
			variable_set_id = (VARIABLE_SET)eResolveProxy(oldVariable_set_id);
			if (variable_set_id != oldVariable_set_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
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
	@Override
	public void setVariable_set_id(VARIABLE_SET newVariable_set_id) {
		VARIABLE_SET oldVariable_set_id = variable_set_id;
		variable_set_id = newVariable_set_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID:
				if (resolve) return getMember_id();
				return basicGetMember_id();
			case Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID:
				if (resolve) return getSubdomain_id();
				return basicGetSubdomain_id();
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID:
				if (resolve) return getVariable_id();
				return basicGetVariable_id();
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID:
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
			case Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID:
				setMember_id((MEMBER)newValue);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)newValue);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)newValue);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID:
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
			case Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID:
				setMember_id((MEMBER)null);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)null);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)null);
				return;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID:
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
			case Data_definitionPackage.COMBINATION_ITEM__MEMBER_ID:
				return member_id != null;
			case Data_definitionPackage.COMBINATION_ITEM__SUBDOMAIN_ID:
				return subdomain_id != null;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_ID:
				return variable_id != null;
			case Data_definitionPackage.COMBINATION_ITEM__VARIABLE_SET_ID:
				return variable_set_id != null;
		}
		return super.eIsSet(featureID);
	}

} //COMBINATION_ITEMImpl
