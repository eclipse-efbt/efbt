/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Resolved Struct Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ResolvedStructColumnParameterImpl extends ParameterImpl implements ResolvedStructColumnParameter
{
	/**
	 * The cached value of the '{@link #getColumnInsideStruct() <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnInsideStruct()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE columnInsideStruct;

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
	protected ResolvedStructColumnParameterImpl()
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
		return Advanced_variable_lineagefunctionsPackage.Literals.RESOLVED_STRUCT_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE getColumnInsideStruct()
	{
		if (columnInsideStruct != null && columnInsideStruct.eIsProxy()) {
			InternalEObject oldColumnInsideStruct = (InternalEObject)columnInsideStruct;
			columnInsideStruct = (VARIABLE)eResolveProxy(oldColumnInsideStruct);
			if (columnInsideStruct != oldColumnInsideStruct) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
			}
		}
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetColumnInsideStruct()
	{
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumnInsideStruct(VARIABLE newColumnInsideStruct)
	{
		VARIABLE oldColumnInsideStruct = columnInsideStruct;
		columnInsideStruct = newColumnInsideStruct;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				if (resolve) return getColumnInsideStruct();
				return basicGetColumnInsideStruct();
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((VARIABLE)newValue);
				return;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((VARIABLE)null);
				return;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				return columnInsideStruct != null;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
				return cubeColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //ResolvedStructColumnParameterImpl
