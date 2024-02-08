/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELOperation;
import org.eclipse.efbt.regdna.model.regdna.SelectColumn;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Select Column</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnImpl#getAsAttribute <em>As Attribute</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SelectColumnImpl extends MinimalEObjectImpl.Container implements SelectColumn {
	/**
	 * The cached value of the '{@link #getAsAttribute() <em>As Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAsAttribute()
	 * @generated
	 * @ordered
	 */
	protected ELOperation asAttribute;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectColumnImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getSelectColumn();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELOperation getAsAttribute() {
		if (asAttribute != null && asAttribute.eIsProxy()) {
			InternalEObject oldAsAttribute = (InternalEObject)asAttribute;
			asAttribute = (ELOperation)eResolveProxy(oldAsAttribute);
			if (asAttribute != oldAsAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE, oldAsAttribute, asAttribute));
			}
		}
		return asAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELOperation basicGetAsAttribute() {
		return asAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAsAttribute(ELOperation newAsAttribute) {
		ELOperation oldAsAttribute = asAttribute;
		asAttribute = newAsAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE, oldAsAttribute, asAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE:
				if (resolve) return getAsAttribute();
				return basicGetAsAttribute();
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
			case regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE:
				setAsAttribute((ELOperation)newValue);
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
			case regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE:
				setAsAttribute((ELOperation)null);
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
			case regdnaPackage.SELECT_COLUMN__AS_ATTRIBUTE:
				return asAttribute != null;
		}
		return super.eIsSet(featureID);
	}

} //SelectColumnImpl
