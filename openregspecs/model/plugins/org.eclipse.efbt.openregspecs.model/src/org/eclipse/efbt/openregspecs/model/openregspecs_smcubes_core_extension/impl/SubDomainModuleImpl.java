/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Sub Domain Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SubDomainModuleImpl#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SubDomainModuleImpl extends ModuleImpl implements SubDomainModule {
	/**
	 * The cached value of the '{@link #getSubdomains() <em>Subdomains</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomains()
	 * @generated
	 * @ordered
	 */
	protected EList<SUBDOMAIN> subdomains;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SubDomainModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return openregspecs_smcubes_core_extensionPackage.Literals.SUB_DOMAIN_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SUBDOMAIN> getSubdomains() {
		if (subdomains == null) {
			subdomains = new EObjectContainmentEList<SUBDOMAIN>(SUBDOMAIN.class, this, openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS);
		}
		return subdomains;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS:
				return ((InternalEList<?>)getSubdomains()).basicRemove(otherEnd, msgs);
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
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS:
				return getSubdomains();
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
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS:
				getSubdomains().clear();
				getSubdomains().addAll((Collection<? extends SUBDOMAIN>)newValue);
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
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS:
				getSubdomains().clear();
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
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE__SUBDOMAINS:
				return subdomains != null && !subdomains.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SubDomainModuleImpl
