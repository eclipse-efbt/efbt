/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityModuleImpl#getBaseEntities <em>Base Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityModuleImpl extends ModuleImpl implements BaseEntityModule {
	/**
	 * The cached value of the '{@link #getBaseEntities() <em>Base Entities</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseEntity> baseEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseEntityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.BASE_ENTITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BaseEntity> getBaseEntities() {
		if (baseEntities == null) {
			baseEntities = new EObjectContainmentEList<BaseEntity>(BaseEntity.class, this, Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES);
		}
		return baseEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return ((InternalEList<?>)getBaseEntities()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return getBaseEntities();
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				getBaseEntities().clear();
				getBaseEntities().addAll((Collection<? extends BaseEntity>)newValue);
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				getBaseEntities().clear();
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return baseEntities != null && !baseEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BaseEntityModuleImpl
