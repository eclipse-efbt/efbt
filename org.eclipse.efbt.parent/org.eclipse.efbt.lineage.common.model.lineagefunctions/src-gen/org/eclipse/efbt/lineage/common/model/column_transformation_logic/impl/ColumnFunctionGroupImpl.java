/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Column Function Group</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionGroupImpl#getColumnFunctions <em>Column Functions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ColumnFunctionGroupImpl extends MinimalEObjectImpl.Container implements ColumnFunctionGroup
{
	/**
	 * The cached value of the '{@link #getColumnFunctions() <em>Column Functions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnFunctions()
	 * @generated
	 * @ordered
	 */
	protected EList<ColumnFunction> columnFunctions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ColumnFunctionGroupImpl()
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
		return Column_transformation_logicPackage.Literals.COLUMN_FUNCTION_GROUP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ColumnFunction> getColumnFunctions()
	{
		if (columnFunctions == null)
		{
			columnFunctions = new EObjectContainmentEList<ColumnFunction>(ColumnFunction.class, this, Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS);
		}
		return columnFunctions;
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
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS:
				return ((InternalEList<?>)getColumnFunctions()).basicRemove(otherEnd, msgs);
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
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS:
				return getColumnFunctions();
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
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS:
				getColumnFunctions().clear();
				getColumnFunctions().addAll((Collection<? extends ColumnFunction>)newValue);
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
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS:
				getColumnFunctions().clear();
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
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS:
				return columnFunctions != null && !columnFunctions.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ColumnFunctionGroupImpl
