/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite.impl;

import org.eclipse.efbt.openregspecs.model.sql_lite.FromClause;
import org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity;
import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>From Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.FromClauseImpl#getSqlEntities <em>Sql Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FromClauseImpl extends MinimalEObjectImpl.Container implements FromClause {
	/**
	 * The cached value of the '{@link #getSqlEntities() <em>Sql Entities</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSqlEntities()
	 * @generated
	 * @ordered
	 */
	protected SQLEntity sqlEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FromClauseImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.FROM_CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntity getSqlEntities() {
		if (sqlEntities != null && sqlEntities.eIsProxy()) {
			InternalEObject oldSqlEntities = (InternalEObject)sqlEntities;
			sqlEntities = (SQLEntity)eResolveProxy(oldSqlEntities);
			if (sqlEntities != oldSqlEntities) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES, oldSqlEntities, sqlEntities));
			}
		}
		return sqlEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntity basicGetSqlEntities() {
		return sqlEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSqlEntities(SQLEntity newSqlEntities) {
		SQLEntity oldSqlEntities = sqlEntities;
		sqlEntities = newSqlEntities;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES, oldSqlEntities, sqlEntities));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES:
				if (resolve) return getSqlEntities();
				return basicGetSqlEntities();
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
			case Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES:
				setSqlEntities((SQLEntity)newValue);
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
			case Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES:
				setSqlEntities((SQLEntity)null);
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
			case Sql_litePackage.FROM_CLAUSE__SQL_ENTITIES:
				return sqlEntities != null;
		}
		return super.eIsSet(featureID);
	}

} //FromClauseImpl
