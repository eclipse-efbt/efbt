/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule;
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
 * An implementation of the model object '<em><b>Derived Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl#getDerivedEntities <em>Derived Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityModuleImpl extends ModuleImpl implements DerivedEntityModule {
	/**
	 * The cached value of the '{@link #getDerivedEntities() <em>Derived Entities</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDerivedEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<DerivedEntity> derivedEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedEntityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.DERIVED_ENTITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<DerivedEntity> getDerivedEntities() {
		if (derivedEntities == null) {
			derivedEntities = new EObjectContainmentEList<DerivedEntity>(DerivedEntity.class, this, Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES);
		}
		return derivedEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES:
				return ((InternalEList<?>)getDerivedEntities()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES:
				return getDerivedEntities();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES:
				getDerivedEntities().clear();
				getDerivedEntities().addAll((Collection<? extends DerivedEntity>)newValue);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES:
				getDerivedEntities().clear();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE__DERIVED_ENTITIES:
				return derivedEntities != null && !derivedEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityModuleImpl
