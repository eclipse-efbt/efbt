/**
 */
package cubes.impl;

import cubes.BaseDeltaCube;
import cubes.CubesPackage;
import cubes.FreeBirdToolsCube;

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
 *   <li>{@link cubes.impl.BaseDeltaCubeImpl#getUltimateBaseCube <em>Ultimate Base Cube</em>}</li>
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
	protected FreeBirdToolsCube ultimateBaseCube;

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
		return CubesPackage.Literals.BASE_DELTA_CUBE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getUltimateBaseCube()
	{
		if (ultimateBaseCube != null && ultimateBaseCube.eIsProxy())
		{
			InternalEObject oldUltimateBaseCube = (InternalEObject)ultimateBaseCube;
			ultimateBaseCube = (FreeBirdToolsCube)eResolveProxy(oldUltimateBaseCube);
			if (ultimateBaseCube != oldUltimateBaseCube)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE, oldUltimateBaseCube, ultimateBaseCube));
			}
		}
		return ultimateBaseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetUltimateBaseCube()
	{
		return ultimateBaseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUltimateBaseCube(FreeBirdToolsCube newUltimateBaseCube)
	{
		FreeBirdToolsCube oldUltimateBaseCube = ultimateBaseCube;
		ultimateBaseCube = newUltimateBaseCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE, oldUltimateBaseCube, ultimateBaseCube));
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
			case CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
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
		switch (featureID)
		{
			case CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				setUltimateBaseCube((FreeBirdToolsCube)newValue);
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
			case CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				setUltimateBaseCube((FreeBirdToolsCube)null);
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
			case CubesPackage.BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE:
				return ultimateBaseCube != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseDeltaCubeImpl
