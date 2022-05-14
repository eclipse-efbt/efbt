/**
 */
package org.eclipse.efbt.openregspecs.model.module_management.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependencies;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Module Dependencies</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleDependenciesImpl#getTheModules <em>The Modules</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ModuleDependenciesImpl extends MinimalEObjectImpl.Container implements ModuleDependencies {
	/**
	 * The cached value of the '{@link #getTheModules() <em>The Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTheModules()
	 * @generated
	 * @ordered
	 */
	protected EList<ModuleDependency> theModules;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ModuleDependenciesImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Module_managementPackage.Literals.MODULE_DEPENDENCIES;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ModuleDependency> getTheModules() {
		if (theModules == null) {
			theModules = new EObjectContainmentEList<ModuleDependency>(ModuleDependency.class, this, Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES);
		}
		return theModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES:
				return ((InternalEList<?>)getTheModules()).basicRemove(otherEnd, msgs);
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
			case Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES:
				return getTheModules();
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
			case Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES:
				getTheModules().clear();
				getTheModules().addAll((Collection<? extends ModuleDependency>)newValue);
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
			case Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES:
				getTheModules().clear();
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
			case Module_managementPackage.MODULE_DEPENDENCIES__THE_MODULES:
				return theModules != null && !theModules.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ModuleDependenciesImpl
