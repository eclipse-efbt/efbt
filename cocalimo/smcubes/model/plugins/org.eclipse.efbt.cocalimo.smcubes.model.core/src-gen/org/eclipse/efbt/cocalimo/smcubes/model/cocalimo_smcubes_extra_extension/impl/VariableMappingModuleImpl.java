/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Variable Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.VariableMappingModuleImpl#getVariableMappings <em>Variable Mappings</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VariableMappingModuleImpl extends ModuleImpl implements VariableMappingModule {
	/**
	 * The cached value of the '{@link #getVariableMappings() <em>Variable Mappings</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariableMappings()
	 * @generated
	 * @ordered
	 */
	protected EList<VARIABLE_MAPPING> variableMappings;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VariableMappingModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Cocalimo_smcubes_extra_extensionPackage.Literals.VARIABLE_MAPPING_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VARIABLE_MAPPING> getVariableMappings() {
		if (variableMappings == null) {
			variableMappings = new EObjectContainmentEList<VARIABLE_MAPPING>(VARIABLE_MAPPING.class, this, Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS);
		}
		return variableMappings;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS:
				return ((InternalEList<?>)getVariableMappings()).basicRemove(otherEnd, msgs);
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
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS:
				return getVariableMappings();
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
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS:
				getVariableMappings().clear();
				getVariableMappings().addAll((Collection<? extends VARIABLE_MAPPING>)newValue);
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
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS:
				getVariableMappings().clear();
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
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS:
				return variableMappings != null && !variableMappings.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VariableMappingModuleImpl
