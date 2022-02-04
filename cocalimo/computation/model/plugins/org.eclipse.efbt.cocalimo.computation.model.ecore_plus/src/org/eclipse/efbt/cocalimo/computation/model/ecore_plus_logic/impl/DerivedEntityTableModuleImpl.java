/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule;
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
 * An implementation of the model object '<em><b>Derived Entity Table Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableModuleImpl#getDerivedEntityTables <em>Derived Entity Tables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityTableModuleImpl extends ModuleImpl implements DerivedEntityTableModule {
	/**
	 * The cached value of the '{@link #getDerivedEntityTables() <em>Derived Entity Tables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDerivedEntityTables()
	 * @generated
	 * @ordered
	 */
	protected EList<DerivedEntityTable> derivedEntityTables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedEntityTableModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.DERIVED_ENTITY_TABLE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<DerivedEntityTable> getDerivedEntityTables() {
		if (derivedEntityTables == null) {
			derivedEntityTables = new EObjectContainmentEList<DerivedEntityTable>(DerivedEntityTable.class, this, Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES);
		}
		return derivedEntityTables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES:
				return ((InternalEList<?>)getDerivedEntityTables()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES:
				return getDerivedEntityTables();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES:
				getDerivedEntityTables().clear();
				getDerivedEntityTables().addAll((Collection<? extends DerivedEntityTable>)newValue);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES:
				getDerivedEntityTables().clear();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES:
				return derivedEntityTables != null && !derivedEntityTables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityTableModuleImpl
