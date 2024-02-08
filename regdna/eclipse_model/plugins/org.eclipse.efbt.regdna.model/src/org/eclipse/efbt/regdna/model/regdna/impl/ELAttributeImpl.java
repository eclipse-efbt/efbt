/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.ELDataType;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Attribute</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAttributeImpl#isID <em>ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAttributeImpl#getEAttributeType <em>EAttribute Type</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELAttributeImpl extends ELStructuralFeatureImpl implements ELAttribute {
	/**
	 * The default value of the '{@link #isID() <em>ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isID()
	 * @generated
	 * @ordered
	 */
	protected static final boolean ID_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isID() <em>ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isID()
	 * @generated
	 * @ordered
	 */
	protected boolean iD = ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getEAttributeType() <em>EAttribute Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEAttributeType()
	 * @generated
	 * @ordered
	 */
	protected ELDataType eAttributeType;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELAttributeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELAttribute();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isID() {
		return iD;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setID(boolean newID) {
		boolean oldID = iD;
		iD = newID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_ATTRIBUTE__ID, oldID, iD));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELDataType getEAttributeType() {
		if (eAttributeType != null && eAttributeType.eIsProxy()) {
			InternalEObject oldEAttributeType = (InternalEObject)eAttributeType;
			eAttributeType = (ELDataType)eResolveProxy(oldEAttributeType);
			if (eAttributeType != oldEAttributeType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE, oldEAttributeType, eAttributeType));
			}
		}
		return eAttributeType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELDataType basicGetEAttributeType() {
		return eAttributeType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEAttributeType(ELDataType newEAttributeType) {
		ELDataType oldEAttributeType = eAttributeType;
		eAttributeType = newEAttributeType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE, oldEAttributeType, eAttributeType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.EL_ATTRIBUTE__ID:
				return isID();
			case regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE:
				if (resolve) return getEAttributeType();
				return basicGetEAttributeType();
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
			case regdnaPackage.EL_ATTRIBUTE__ID:
				setID((Boolean)newValue);
				return;
			case regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE:
				setEAttributeType((ELDataType)newValue);
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
			case regdnaPackage.EL_ATTRIBUTE__ID:
				setID(ID_EDEFAULT);
				return;
			case regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE:
				setEAttributeType((ELDataType)null);
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
			case regdnaPackage.EL_ATTRIBUTE__ID:
				return iD != ID_EDEFAULT;
			case regdnaPackage.EL_ATTRIBUTE__EATTRIBUTE_TYPE:
				return eAttributeType != null;
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
		result.append(" (iD: ");
		result.append(iD);
		result.append(')');
		return result.toString();
	}

} //ELAttributeImpl
