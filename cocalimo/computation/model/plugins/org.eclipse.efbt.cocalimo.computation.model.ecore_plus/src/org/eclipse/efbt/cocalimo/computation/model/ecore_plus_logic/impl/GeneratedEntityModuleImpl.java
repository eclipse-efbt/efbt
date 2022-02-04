/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Generated Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl#getGeneratedEntities <em>Generated Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GeneratedEntityModuleImpl extends ModuleImpl implements GeneratedEntityModule {
	/**
	 * The cached value of the '{@link #getGeneratedEntities() <em>Generated Entities</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGeneratedEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<GeneratedEntity> generatedEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GeneratedEntityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.GENERATED_ENTITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<GeneratedEntity> getGeneratedEntities() {
		if (generatedEntities == null) {
			generatedEntities = new EObjectContainmentEList<GeneratedEntity>(GeneratedEntity.class, this, Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES);
		}
		return generatedEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES:
				return ((InternalEList<?>)getGeneratedEntities()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES:
				return getGeneratedEntities();
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
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES:
				getGeneratedEntities().clear();
				getGeneratedEntities().addAll((Collection<? extends GeneratedEntity>)newValue);
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
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES:
				getGeneratedEntities().clear();
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
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE__GENERATED_ENTITIES:
				return generatedEntities != null && !generatedEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //GeneratedEntityModuleImpl
