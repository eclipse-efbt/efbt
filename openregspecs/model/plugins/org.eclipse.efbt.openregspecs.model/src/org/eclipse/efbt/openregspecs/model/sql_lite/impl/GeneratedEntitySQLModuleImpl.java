/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL;
import org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule;
import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Generated Entity SQL Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLModuleImpl#getGeneratedEntitySQLs <em>Generated Entity SQ Ls</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GeneratedEntitySQLModuleImpl extends ModuleImpl implements GeneratedEntitySQLModule {
	/**
	 * The cached value of the '{@link #getGeneratedEntitySQLs() <em>Generated Entity SQ Ls</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGeneratedEntitySQLs()
	 * @generated
	 * @ordered
	 */
	protected EList<GeneratedEntitySQL> generatedEntitySQLs;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GeneratedEntitySQLModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.GENERATED_ENTITY_SQL_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<GeneratedEntitySQL> getGeneratedEntitySQLs() {
		if (generatedEntitySQLs == null) {
			generatedEntitySQLs = new EObjectContainmentEList<GeneratedEntitySQL>(GeneratedEntitySQL.class, this, Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS);
		}
		return generatedEntitySQLs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS:
				return ((InternalEList<?>)getGeneratedEntitySQLs()).basicRemove(otherEnd, msgs);
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
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS:
				return getGeneratedEntitySQLs();
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
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS:
				getGeneratedEntitySQLs().clear();
				getGeneratedEntitySQLs().addAll((Collection<? extends GeneratedEntitySQL>)newValue);
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
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS:
				getGeneratedEntitySQLs().clear();
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
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS:
				return generatedEntitySQLs != null && !generatedEntitySQLs.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //GeneratedEntitySQLModuleImpl
