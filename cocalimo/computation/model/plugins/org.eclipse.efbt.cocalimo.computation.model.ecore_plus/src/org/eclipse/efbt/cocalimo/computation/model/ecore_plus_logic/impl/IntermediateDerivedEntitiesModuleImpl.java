/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Intermediate Derived Entities Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl#getIntermediateDerivedEntities <em>Intermediate Derived Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class IntermediateDerivedEntitiesModuleImpl extends ModuleImpl implements IntermediateDerivedEntitiesModule {
	/**
	 * The cached value of the '{@link #getIntermediateDerivedEntities() <em>Intermediate Derived Entities</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIntermediateDerivedEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<IntermediateDerivedEntity> intermediateDerivedEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected IntermediateDerivedEntitiesModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.INTERMEDIATE_DERIVED_ENTITIES_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<IntermediateDerivedEntity> getIntermediateDerivedEntities() {
		if (intermediateDerivedEntities == null) {
			intermediateDerivedEntities = new EObjectContainmentEList<IntermediateDerivedEntity>(IntermediateDerivedEntity.class, this, Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES);
		}
		return intermediateDerivedEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES:
				return ((InternalEList<?>)getIntermediateDerivedEntities()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES:
				return getIntermediateDerivedEntities();
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
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES:
				getIntermediateDerivedEntities().clear();
				getIntermediateDerivedEntities().addAll((Collection<? extends IntermediateDerivedEntity>)newValue);
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
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES:
				getIntermediateDerivedEntities().clear();
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
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES:
				return intermediateDerivedEntities != null && !intermediateDerivedEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //IntermediateDerivedEntitiesModuleImpl
