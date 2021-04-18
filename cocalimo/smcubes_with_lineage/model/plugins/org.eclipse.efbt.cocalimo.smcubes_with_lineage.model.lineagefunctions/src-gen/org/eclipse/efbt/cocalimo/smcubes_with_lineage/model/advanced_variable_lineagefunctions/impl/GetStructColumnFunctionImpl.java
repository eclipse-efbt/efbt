/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Get Struct Column Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl#getColumnInStruct <em>Column In Struct</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GetStructColumnFunctionImpl extends ColumnFunctionImpl implements GetStructColumnFunction
{
	/**
	 * The cached value of the '{@link #getColumnInStruct() <em>Column In Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnInStruct()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE columnInStruct;

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
	protected GetStructColumnFunctionImpl()
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
		return Advanced_variable_lineagefunctionsPackage.Literals.GET_STRUCT_COLUMN_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE getColumnInStruct()
	{
		if (columnInStruct != null && columnInStruct.eIsProxy()) {
			InternalEObject oldColumnInStruct = (InternalEObject)columnInStruct;
			columnInStruct = (VARIABLE)eResolveProxy(oldColumnInStruct);
			if (columnInStruct != oldColumnInStruct) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT, oldColumnInStruct, columnInStruct));
			}
		}
		return columnInStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetColumnInStruct()
	{
		return columnInStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumnInStruct(VARIABLE newColumnInStruct)
	{
		VARIABLE oldColumnInStruct = columnInStruct;
		columnInStruct = newColumnInStruct;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT, oldColumnInStruct, columnInStruct));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubeColumn getCubeColumn()
	{
		if (cubeColumn != null && cubeColumn.eIsProxy()) {
			InternalEObject oldCubeColumn = (InternalEObject)cubeColumn;
			cubeColumn = (CubeColumn)eResolveProxy(oldCubeColumn);
			if (cubeColumn != oldCubeColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
	public void setCubeColumn(CubeColumn newCubeColumn)
	{
		CubeColumn oldCubeColumn = cubeColumn;
		cubeColumn = newCubeColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT:
				if (resolve) return getColumnInStruct();
				return basicGetColumnInStruct();
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN:
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
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT:
				setColumnInStruct((VARIABLE)newValue);
				return;
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN:
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
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT:
				setColumnInStruct((VARIABLE)null);
				return;
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN:
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
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT:
				return columnInStruct != null;
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN:
				return cubeColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //GetStructColumnFunctionImpl
