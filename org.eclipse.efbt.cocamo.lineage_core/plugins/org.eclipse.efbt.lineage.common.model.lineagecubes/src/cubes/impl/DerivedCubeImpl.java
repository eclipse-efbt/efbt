/**
 */
package cubes.impl;

import cubes.CubesPackage;
import cubes.DerivedCube;
import cubes.FreeBirdToolsCube;

import java.util.Collection;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Cube</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link cubes.impl.DerivedCubeImpl#getSourceCubes <em>Source Cubes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedCubeImpl extends FreeBirdToolsCubeImpl implements DerivedCube
{
	/**
	 * The cached value of the '{@link #getSourceCubes() <em>Source Cubes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceCubes()
	 * @generated
	 * @ordered
	 */
	protected EList<FreeBirdToolsCube> sourceCubes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedCubeImpl()
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
		return CubesPackage.Literals.DERIVED_CUBE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FreeBirdToolsCube> getSourceCubes()
	{
		if (sourceCubes == null)
		{
			sourceCubes = new EObjectResolvingEList<FreeBirdToolsCube>(FreeBirdToolsCube.class, this, CubesPackage.DERIVED_CUBE__SOURCE_CUBES);
		}
		return sourceCubes;
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
			case CubesPackage.DERIVED_CUBE__SOURCE_CUBES:
				return getSourceCubes();
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
			case CubesPackage.DERIVED_CUBE__SOURCE_CUBES:
				getSourceCubes().clear();
				getSourceCubes().addAll((Collection<? extends FreeBirdToolsCube>)newValue);
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
			case CubesPackage.DERIVED_CUBE__SOURCE_CUBES:
				getSourceCubes().clear();
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
			case CubesPackage.DERIVED_CUBE__SOURCE_CUBES:
				return sourceCubes != null && !sourceCubes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedCubeImpl
