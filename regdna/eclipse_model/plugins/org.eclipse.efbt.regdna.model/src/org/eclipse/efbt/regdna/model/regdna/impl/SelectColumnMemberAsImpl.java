/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral;
import org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Select Column Member As</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnMemberAsImpl#getMemberAsConstant <em>Member As Constant</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SelectColumnMemberAsImpl extends SelectColumnImpl implements SelectColumnMemberAs {
	/**
	 * The cached value of the '{@link #getMemberAsConstant() <em>Member As Constant</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberAsConstant()
	 * @generated
	 * @ordered
	 */
	protected ELEnumLiteral memberAsConstant;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectColumnMemberAsImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getSelectColumnMemberAs();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELEnumLiteral getMemberAsConstant() {
		if (memberAsConstant != null && memberAsConstant.eIsProxy()) {
			InternalEObject oldMemberAsConstant = (InternalEObject)memberAsConstant;
			memberAsConstant = (ELEnumLiteral)eResolveProxy(oldMemberAsConstant);
			if (memberAsConstant != oldMemberAsConstant) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT, oldMemberAsConstant, memberAsConstant));
			}
		}
		return memberAsConstant;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELEnumLiteral basicGetMemberAsConstant() {
		return memberAsConstant;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMemberAsConstant(ELEnumLiteral newMemberAsConstant) {
		ELEnumLiteral oldMemberAsConstant = memberAsConstant;
		memberAsConstant = newMemberAsConstant;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT, oldMemberAsConstant, memberAsConstant));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT:
				if (resolve) return getMemberAsConstant();
				return basicGetMemberAsConstant();
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
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT:
				setMemberAsConstant((ELEnumLiteral)newValue);
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
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT:
				setMemberAsConstant((ELEnumLiteral)null);
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
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT:
				return memberAsConstant != null;
		}
		return super.eIsSet(featureID);
	}

} //SelectColumnMemberAsImpl
