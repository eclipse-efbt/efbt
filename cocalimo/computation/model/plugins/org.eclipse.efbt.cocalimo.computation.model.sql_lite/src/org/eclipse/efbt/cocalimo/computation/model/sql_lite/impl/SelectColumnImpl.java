/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Select Column</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.SelectColumnImpl#getAs <em>As</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.SelectColumnImpl#getMemberAsConstant <em>Member As Constant</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SelectColumnImpl extends ColumnImpl implements SelectColumn {
	/**
	 * The cached value of the '{@link #getAs() <em>As</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAs()
	 * @generated
	 * @ordered
	 */
	protected Attribute as;

	/**
	 * The cached value of the '{@link #getMemberAsConstant() <em>Member As Constant</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberAsConstant()
	 * @generated
	 * @ordered
	 */
	protected MEMBER memberAsConstant;

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
		return Sql_litePackage.Literals.SELECT_COLUMN;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute getAs() {
		if (as != null && as.eIsProxy()) {
			InternalEObject oldAs = (InternalEObject)as;
			as = (Attribute)eResolveProxy(oldAs);
			if (as != oldAs) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.SELECT_COLUMN__AS, oldAs, as));
			}
		}
		return as;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute basicGetAs() {
		return as;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAs(Attribute newAs) {
		Attribute oldAs = as;
		as = newAs;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.SELECT_COLUMN__AS, oldAs, as));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER getMemberAsConstant() {
		if (memberAsConstant != null && memberAsConstant.eIsProxy()) {
			InternalEObject oldMemberAsConstant = (InternalEObject)memberAsConstant;
			memberAsConstant = (MEMBER)eResolveProxy(oldMemberAsConstant);
			if (memberAsConstant != oldMemberAsConstant) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT, oldMemberAsConstant, memberAsConstant));
			}
		}
		return memberAsConstant;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetMemberAsConstant() {
		return memberAsConstant;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMemberAsConstant(MEMBER newMemberAsConstant) {
		MEMBER oldMemberAsConstant = memberAsConstant;
		memberAsConstant = newMemberAsConstant;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT, oldMemberAsConstant, memberAsConstant));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Sql_litePackage.SELECT_COLUMN__AS:
				if (resolve) return getAs();
				return basicGetAs();
			case Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT:
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
			case Sql_litePackage.SELECT_COLUMN__AS:
				setAs((Attribute)newValue);
				return;
			case Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT:
				setMemberAsConstant((MEMBER)newValue);
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
			case Sql_litePackage.SELECT_COLUMN__AS:
				setAs((Attribute)null);
				return;
			case Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT:
				setMemberAsConstant((MEMBER)null);
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
			case Sql_litePackage.SELECT_COLUMN__AS:
				return as != null;
			case Sql_litePackage.SELECT_COLUMN__MEMBER_AS_CONSTANT:
				return memberAsConstant != null;
		}
		return super.eIsSet(featureID);
	}

} //SelectColumnImpl
