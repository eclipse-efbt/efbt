/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions.impl;

import org.eclipse.efbt.common.model.functions.impl.ParameterImpl;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Speculative Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SpeculativeCubeColumnParameterImpl extends ParameterImpl implements SpeculativeCubeColumnParameter
{
	/**
	 * The cached value of the '{@link #getColumn() <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumn()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE column;

	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube cube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SpeculativeCubeColumnParameterImpl()
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
		return LineagefunctionsPackage.Literals.SPECULATIVE_CUBE_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getColumn()
	{
		if (column != null && column.eIsProxy())
		{
			InternalEObject oldColumn = (InternalEObject)column;
			column = (VARIABLE)eResolveProxy(oldColumn);
			if (column != oldColumn)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN, oldColumn, column));
			}
		}
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetColumn()
	{
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setColumn(VARIABLE newColumn)
	{
		VARIABLE oldColumn = column;
		column = newColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN, oldColumn, column));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getCube()
	{
		if (cube != null && cube.eIsProxy())
		{
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (FreeBirdToolsCube)eResolveProxy(oldCube);
			if (cube != oldCube)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetCube()
	{
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(FreeBirdToolsCube newCube)
	{
		FreeBirdToolsCube oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE, oldCube, cube));
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
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN:
				if (resolve) return getColumn();
				return basicGetColumn();
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
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
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN:
				setColumn((VARIABLE)newValue);
				return;
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				setCube((FreeBirdToolsCube)newValue);
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
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN:
				setColumn((VARIABLE)null);
				return;
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				setCube((FreeBirdToolsCube)null);
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
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN:
				return column != null;
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				return cube != null;
		}
		return super.eIsSet(featureID);
	}

} //SpeculativeCubeColumnParameterImpl
