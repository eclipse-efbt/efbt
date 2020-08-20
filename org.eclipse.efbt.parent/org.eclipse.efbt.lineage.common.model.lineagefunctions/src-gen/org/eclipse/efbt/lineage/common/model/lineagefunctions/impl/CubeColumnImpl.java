/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions.impl;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cube Column</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl#getColumnID <em>Column ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl#isUsedInSubsets <em>Used In Subsets</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CubeColumnImpl extends MinimalEObjectImpl.Container implements CubeColumn
{
	/**
	 * The default value of the '{@link #getColumnID() <em>Column ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnID()
	 * @generated
	 * @ordered
	 */
	protected static final String COLUMN_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getColumnID() <em>Column ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnID()
	 * @generated
	 * @ordered
	 */
	protected String columnID = COLUMN_ID_EDEFAULT;

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
	 * The default value of the '{@link #isUsedInSubsets() <em>Used In Subsets</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isUsedInSubsets()
	 * @generated
	 * @ordered
	 */
	protected static final boolean USED_IN_SUBSETS_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isUsedInSubsets() <em>Used In Subsets</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isUsedInSubsets()
	 * @generated
	 * @ordered
	 */
	protected boolean usedInSubsets = USED_IN_SUBSETS_EDEFAULT;

	/**
	 * The cached value of the '{@link #getVariable() <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CubeColumnImpl()
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
		return LineagefunctionsPackage.Literals.CUBE_COLUMN;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getColumnID()
	{
		return columnID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setColumnID(String newColumnID)
	{
		String oldColumnID = columnID;
		columnID = newColumnID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID, oldColumnID, columnID));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, LineagefunctionsPackage.CUBE_COLUMN__CUBE, oldCube, cube));
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
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.CUBE_COLUMN__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isUsedInSubsets()
	{
		return usedInSubsets;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUsedInSubsets(boolean newUsedInSubsets)
	{
		boolean oldUsedInSubsets = usedInSubsets;
		usedInSubsets = newUsedInSubsets;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS, oldUsedInSubsets, usedInSubsets));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getVariable()
	{
		if (variable != null && variable.eIsProxy())
		{
			InternalEObject oldVariable = (InternalEObject)variable;
			variable = (VARIABLE)eResolveProxy(oldVariable);
			if (variable != oldVariable)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, LineagefunctionsPackage.CUBE_COLUMN__VARIABLE, oldVariable, variable));
			}
		}
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVariable()
	{
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariable(VARIABLE newVariable)
	{
		VARIABLE oldVariable = variable;
		variable = newVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, LineagefunctionsPackage.CUBE_COLUMN__VARIABLE, oldVariable, variable));
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
			case LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID:
				return getColumnID();
			case LineagefunctionsPackage.CUBE_COLUMN__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS:
				return isUsedInSubsets();
			case LineagefunctionsPackage.CUBE_COLUMN__VARIABLE:
				if (resolve) return getVariable();
				return basicGetVariable();
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
			case LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID:
				setColumnID((String)newValue);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__CUBE:
				setCube((FreeBirdToolsCube)newValue);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS:
				setUsedInSubsets((Boolean)newValue);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__VARIABLE:
				setVariable((VARIABLE)newValue);
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
			case LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID:
				setColumnID(COLUMN_ID_EDEFAULT);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__CUBE:
				setCube((FreeBirdToolsCube)null);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS:
				setUsedInSubsets(USED_IN_SUBSETS_EDEFAULT);
				return;
			case LineagefunctionsPackage.CUBE_COLUMN__VARIABLE:
				setVariable((VARIABLE)null);
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
			case LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID:
				return COLUMN_ID_EDEFAULT == null ? columnID != null : !COLUMN_ID_EDEFAULT.equals(columnID);
			case LineagefunctionsPackage.CUBE_COLUMN__CUBE:
				return cube != null;
			case LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS:
				return usedInSubsets != USED_IN_SUBSETS_EDEFAULT;
			case LineagefunctionsPackage.CUBE_COLUMN__VARIABLE:
				return variable != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (columnID: ");
		result.append(columnID);
		result.append(", usedInSubsets: ");
		result.append(usedInSubsets);
		result.append(')');
		return result.toString();
	}

} //CubeColumnImpl
