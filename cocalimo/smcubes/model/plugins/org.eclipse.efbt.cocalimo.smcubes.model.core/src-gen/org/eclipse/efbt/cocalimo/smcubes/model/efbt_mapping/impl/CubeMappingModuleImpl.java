/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.CubeMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cube Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl.CubeMappingModuleImpl#getCubeMappings <em>Cube Mappings</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CubeMappingModuleImpl extends ModuleImpl implements CubeMappingModule
{
	/**
	 * The cached value of the '{@link #getCubeMappings() <em>Cube Mappings</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeMappings()
	 * @generated
	 * @ordered
	 */
	protected EList<CUBE_MAPPING> cubeMappings;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CubeMappingModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Efbt_mappingPackage.Literals.CUBE_MAPPING_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CUBE_MAPPING> getCubeMappings()
	{
		if (cubeMappings == null) {
			cubeMappings = new EObjectContainmentEList<CUBE_MAPPING>(CUBE_MAPPING.class, this, Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS);
		}
		return cubeMappings;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS:
				return ((InternalEList<?>)getCubeMappings()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS:
				return getCubeMappings();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS:
				getCubeMappings().clear();
				getCubeMappings().addAll((Collection<? extends CUBE_MAPPING>)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS:
				getCubeMappings().clear();
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE__CUBE_MAPPINGS:
				return cubeMappings != null && !cubeMappings.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CubeMappingModuleImpl
