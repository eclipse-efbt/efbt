/**
 */
package org.eclipse.efbt.lineage.common.model.cubes.impl;

import java.util.Collection;

import org.eclipse.efbt.common.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;
import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCubeModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Free Bird Tools Cube Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.cubes.impl.FreeBirdToolsCubeModuleImpl#getCubes <em>Cubes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FreeBirdToolsCubeModuleImpl extends ModuleImpl implements FreeBirdToolsCubeModule
{
	/**
	 * The cached value of the '{@link #getCubes() <em>Cubes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubes()
	 * @generated
	 * @ordered
	 */
	protected EList<FreeBirdToolsCube> cubes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FreeBirdToolsCubeModuleImpl()
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
		return CubesPackage.Literals.FREE_BIRD_TOOLS_CUBE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FreeBirdToolsCube> getCubes()
	{
		if (cubes == null)
		{
			cubes = new EObjectContainmentEList<FreeBirdToolsCube>(FreeBirdToolsCube.class, this, CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES);
		}
		return cubes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES:
				return ((InternalEList<?>)getCubes()).basicRemove(otherEnd, msgs);
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
		switch (featureID)
		{
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES:
				return getCubes();
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
		switch (featureID)
		{
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES:
				getCubes().clear();
				getCubes().addAll((Collection<? extends FreeBirdToolsCube>)newValue);
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
		switch (featureID)
		{
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES:
				getCubes().clear();
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
		switch (featureID)
		{
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE__CUBES:
				return cubes != null && !cubes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FreeBirdToolsCubeModuleImpl
