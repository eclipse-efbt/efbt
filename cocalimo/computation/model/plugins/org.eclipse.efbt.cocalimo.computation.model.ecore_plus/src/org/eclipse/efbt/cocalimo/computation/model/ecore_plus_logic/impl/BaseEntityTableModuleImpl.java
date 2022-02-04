/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Entity Table Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableModuleImpl#getBaseEntityTables <em>Base Entity Tables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityTableModuleImpl extends ModuleImpl implements BaseEntityTableModule {
	/**
	 * The cached value of the '{@link #getBaseEntityTables() <em>Base Entity Tables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseEntityTables()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseEntityTable> baseEntityTables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseEntityTableModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASE_ENTITY_TABLE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BaseEntityTable> getBaseEntityTables() {
		if (baseEntityTables == null) {
			baseEntityTables = new EObjectContainmentEList<BaseEntityTable>(BaseEntityTable.class, this, Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES);
		}
		return baseEntityTables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES:
				return ((InternalEList<?>)getBaseEntityTables()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES:
				return getBaseEntityTables();
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
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES:
				getBaseEntityTables().clear();
				getBaseEntityTables().addAll((Collection<? extends BaseEntityTable>)newValue);
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
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES:
				getBaseEntityTables().clear();
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
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES:
				return baseEntityTables != null && !baseEntityTables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BaseEntityTableModuleImpl
