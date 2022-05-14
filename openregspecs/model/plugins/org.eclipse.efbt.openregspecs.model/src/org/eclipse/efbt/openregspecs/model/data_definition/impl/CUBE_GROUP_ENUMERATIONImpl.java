/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.impl;

import java.util.Date;

import org.eclipse.efbt.openregspecs.model.data_definition.CUBE;
import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP;
import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP_ENUMERATION;
import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE GROUP ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl#getCube_group_id <em>Cube group id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl#getCube_id <em>Cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_GROUP_ENUMERATIONImpl extends MinimalEObjectImpl.Container implements CUBE_GROUP_ENUMERATION {
	/**
	 * The cached value of the '{@link #getCube_group_id() <em>Cube group id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_group_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE_GROUP cube_group_id;

	/**
	 * The cached value of the '{@link #getCube_id() <em>Cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube_id;

	/**
	 * The default value of the '{@link #getOrder() <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder()
	 * @generated
	 * @ordered
	 */
	protected static final int ORDER_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getOrder() <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder()
	 * @generated
	 * @ordered
	 */
	protected int order = ORDER_EDEFAULT;

	/**
	 * The default value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_FROM_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected Date valid_from = VALID_FROM_EDEFAULT;

	/**
	 * The default value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_TO_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected Date valid_to = VALID_TO_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_GROUP_ENUMERATIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_GROUP_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_GROUP getCube_group_id() {
		if (cube_group_id != null && cube_group_id.eIsProxy()) {
			InternalEObject oldCube_group_id = (InternalEObject)cube_group_id;
			cube_group_id = (CUBE_GROUP)eResolveProxy(oldCube_group_id);
			if (cube_group_id != oldCube_group_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID, oldCube_group_id, cube_group_id));
			}
		}
		return cube_group_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_GROUP basicGetCube_group_id() {
		return cube_group_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_group_id(CUBE_GROUP newCube_group_id) {
		CUBE_GROUP oldCube_group_id = cube_group_id;
		cube_group_id = newCube_group_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID, oldCube_group_id, cube_group_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE getCube_id() {
		if (cube_id != null && cube_id.eIsProxy()) {
			InternalEObject oldCube_id = (InternalEObject)cube_id;
			cube_id = (CUBE)eResolveProxy(oldCube_id);
			if (cube_id != oldCube_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID, oldCube_id, cube_id));
			}
		}
		return cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube_id() {
		return cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_id(CUBE newCube_id) {
		CUBE oldCube_id = cube_id;
		cube_id = newCube_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID, oldCube_id, cube_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getOrder() {
		return order;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOrder(int newOrder) {
		int oldOrder = order;
		order = newOrder;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP_ENUMERATION__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_from() {
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_from(Date newValid_from) {
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_FROM, oldValid_from, valid_from));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_to() {
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_to(Date newValid_to) {
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID:
				if (resolve) return getCube_group_id();
				return basicGetCube_group_id();
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID:
				if (resolve) return getCube_id();
				return basicGetCube_id();
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__ORDER:
				return getOrder();
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_FROM:
				return getValid_from();
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_TO:
				return getValid_to();
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
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID:
				setCube_group_id((CUBE_GROUP)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID:
				setCube_id((CUBE)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__ORDER:
				setOrder((Integer)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_TO:
				setValid_to((Date)newValue);
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
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID:
				setCube_group_id((CUBE_GROUP)null);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID:
				setCube_id((CUBE)null);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
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
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID:
				return cube_group_id != null;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__CUBE_ID:
				return cube_id != null;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__ORDER:
				return order != ORDER_EDEFAULT;
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (order: ");
		result.append(order);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(')');
		return result.toString();
	}

} //CUBE_GROUP_ENUMERATIONImpl
