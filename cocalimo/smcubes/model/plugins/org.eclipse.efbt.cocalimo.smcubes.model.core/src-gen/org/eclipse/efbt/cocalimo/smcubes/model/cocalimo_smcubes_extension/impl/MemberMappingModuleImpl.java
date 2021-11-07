/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberMappingModule;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Member Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.MemberMappingModuleImpl#getMemberMappings <em>Member Mappings</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MemberMappingModuleImpl extends ModuleImpl implements MemberMappingModule {
	/**
	 * The cached value of the '{@link #getMemberMappings() <em>Member Mappings</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberMappings()
	 * @generated
	 * @ordered
	 */
	protected EList<MEMBER_MAPPING> memberMappings;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MemberMappingModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Cocalimo_smcubes_extensionPackage.Literals.MEMBER_MAPPING_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MEMBER_MAPPING> getMemberMappings() {
		if (memberMappings == null) {
			memberMappings = new EObjectContainmentEList<MEMBER_MAPPING>(MEMBER_MAPPING.class, this, Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS);
		}
		return memberMappings;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS:
				return ((InternalEList<?>)getMemberMappings()).basicRemove(otherEnd, msgs);
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
			case Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS:
				return getMemberMappings();
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
			case Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS:
				getMemberMappings().clear();
				getMemberMappings().addAll((Collection<? extends MEMBER_MAPPING>)newValue);
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
			case Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS:
				getMemberMappings().clear();
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
			case Cocalimo_smcubes_extensionPackage.MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS:
				return memberMappings != null && !memberMappings.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //MemberMappingModuleImpl
