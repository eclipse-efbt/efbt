/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
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
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.SelectColumnImpl#getAs <em>As</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class SelectColumnImpl extends MinimalEObjectImpl.Container implements SelectColumn {
	/**
	 * The cached value of the '{@link #getAs() <em>As</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAs()
	 * @generated
	 * @ordered
	 */
	protected EAttribute as;

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
	public EAttribute getAs() {
		if (as != null && as.eIsProxy()) {
			InternalEObject oldAs = (InternalEObject)as;
			as = (EAttribute)eResolveProxy(oldAs);
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
	public EAttribute basicGetAs() {
		return as;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAs(EAttribute newAs) {
		EAttribute oldAs = as;
		as = newAs;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.SELECT_COLUMN__AS, oldAs, as));
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
				setAs((EAttribute)newValue);
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
				setAs((EAttribute)null);
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
		}
		return super.eIsSet(featureID);
	}

} //SelectColumnImpl
