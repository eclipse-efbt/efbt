/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ResolvedCubeColumnParameterImpl#getCubeColumn <em>Cube Column</em>}</li>
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
		return Column_transformation_logicPackage.Literals.RESOLVED_CUBE_COLUMN_PARAMETER;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
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
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
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
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN:
				return cubeColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //ResolvedCubeColumnParameterImpl
