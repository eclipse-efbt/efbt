/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.ColumnAsFunction;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Column As Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.ColumnAsFunctionImpl#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ColumnAsFunctionImpl extends MinimalEObjectImpl.Container implements ColumnAsFunction {
	/**
	 * The cached value of the '{@link #getColumn() <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumn()
	 * @generated
	 * @ordered
	 */
	protected DerivedAttribute column;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ColumnAsFunctionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.COLUMN_AS_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedAttribute getColumn() {
		if (column != null && column.eIsProxy()) {
			InternalEObject oldColumn = (InternalEObject)column;
			column = (DerivedAttribute)eResolveProxy(oldColumn);
			if (column != oldColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN, oldColumn, column));
			}
		}
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedAttribute basicGetColumn() {
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumn(DerivedAttribute newColumn) {
		DerivedAttribute oldColumn = column;
		column = newColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN, oldColumn, column));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN:
				if (resolve) return getColumn();
				return basicGetColumn();
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
			case Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN:
				setColumn((DerivedAttribute)newValue);
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
			case Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN:
				setColumn((DerivedAttribute)null);
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
			case Sql_litePackage.COLUMN_AS_FUNCTION__COLUMN:
				return column != null;
		}
		return super.eIsSet(featureID);
	}

} //ColumnAsFunctionImpl
