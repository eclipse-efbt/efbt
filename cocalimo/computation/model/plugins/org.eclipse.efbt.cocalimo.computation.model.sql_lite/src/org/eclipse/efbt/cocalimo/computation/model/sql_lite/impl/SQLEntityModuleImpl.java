/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SQLEntity;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SQLEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SQL Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.SQLEntityModuleImpl#getSqlEntities <em>Sql Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SQLEntityModuleImpl extends ModuleImpl implements SQLEntityModule {
	/**
	 * The cached value of the '{@link #getSqlEntities() <em>Sql Entities</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSqlEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<SQLEntity> sqlEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SQLEntityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.SQL_ENTITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SQLEntity> getSqlEntities() {
		if (sqlEntities == null) {
			sqlEntities = new EObjectContainmentEList<SQLEntity>(SQLEntity.class, this, Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES);
		}
		return sqlEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES:
				return ((InternalEList<?>)getSqlEntities()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES:
				return getSqlEntities();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES:
				getSqlEntities().clear();
				getSqlEntities().addAll((Collection<? extends SQLEntity>)newValue);
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
			case Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES:
				getSqlEntities().clear();
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
			case Sql_litePackage.SQL_ENTITY_MODULE__SQL_ENTITIES:
				return sqlEntities != null && !sqlEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SQLEntityModuleImpl
