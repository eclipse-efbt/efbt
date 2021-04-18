/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.BaseCubeImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Delta Cube</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseDeltaCubeImpl#getUltimateBaseCube <em>Ultimate Base Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseDeltaCubeImpl extends BaseCubeImpl implements BaseDeltaCube
{
	/**
	 * The cached value of the '{@link #getUltimateBaseCube() <em>Ultimate Base Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUltimateBaseCube()
	 * @generated
	 * @ordered
	 */
	protected BaseCube ultimateBaseCube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseDeltaCubeImpl()
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
		return Incremental_cubesPackage.Literals.BASE_DELTA_CUBE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseCube getUltimateBaseCube()
	{
		if (ultimateBaseCube != null && ultimateBaseCube.eIsProxy()) {
			InternalEObject oldUltimateBaseCube = (InternalEObject)ultimateBaseCube;
			ultimateBaseCube = (BaseCube)eResolveProxy(oldUltimateBaseCube);
			if (ultimateBaseCube != oldUltimateBaseCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE, oldUltimateBaseCube, ultimateBaseCube));
			}
		}
		return ultimateBaseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseCube basicGetUltimateBaseCube()
	{
		return ultimateBaseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUltimateBaseCube(BaseCube newUltimateBaseCube)
	{
		BaseCube oldUltimateBaseCube = ultimateBaseCube;
		ultimateBaseCube = newUltimateBaseCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE, oldUltimateBaseCube, ultimateBaseCube));
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
			case Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				if (resolve) return getUltimateBaseCube();
				return basicGetUltimateBaseCube();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				setUltimateBaseCube((BaseCube)newValue);
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
			case Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				setUltimateBaseCube((BaseCube)null);
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
			case Incremental_cubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				return ultimateBaseCube != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseDeltaCubeImpl
