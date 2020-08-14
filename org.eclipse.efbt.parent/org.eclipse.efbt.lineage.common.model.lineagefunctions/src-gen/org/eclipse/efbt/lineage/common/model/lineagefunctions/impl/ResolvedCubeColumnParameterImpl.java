/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions.impl;

import org.eclipse.efbt.common.model.functions.impl.ParameterImpl;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Resolved Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ResolvedCubeColumnParameterImpl#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ResolvedCubeColumnParameterImpl extends ParameterImpl implements ResolvedCubeColumnParameter
{
	/**
	 * The cached value of the '{@link #getCubeColumn() <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeColumn()
	 * @generated
	 * @ordered
	 */
	protected CubeColumn cubeColumn;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ResolvedCubeColumnParameterImpl()
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
		return LineagefunctionsPackage.Literals.RESOLVED_CUBE_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeColumn getCubeColumn()
	{
		if (cubeColumn != null && cubeColumn.eIsProxy())
		{
			InternalEObject oldCubeColumn = (InternalEObject)cubeColumn;
			cubeColumn = (CubeColumn)eResolveProxy(oldCubeColumn);
			if (cubeColumn != oldCubeColumn)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
			}
		}
		return cubeColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubeColumn basicGetCubeColumn()
	{
		return cubeColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCubeColumn(CubeColumn newCubeColumn)
	{
		CubeColumn oldCubeColumn = cubeColumn;
		cubeColumn = newCubeColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			case LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
				if (resolve) return getCubeColumn();
				return basicGetCubeColumn();
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
			case LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
				setCubeColumn((CubeColumn)newValue);
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
			case LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
				setCubeColumn((CubeColumn)null);
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
			case LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
				return cubeColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //ResolvedCubeColumnParameterImpl
