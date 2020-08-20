/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl;

import org.eclipse.efbt.common.model.functions.AggregateFunction;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Aggregate Column Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.AggregateColumnFunctionImpl#getAggregateFunction <em>Aggregate Function</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AggregateColumnFunctionImpl extends ColumnFunctionImpl implements AggregateColumnFunction
{
	/**
	 * The cached value of the '{@link #getAggregateFunction() <em>Aggregate Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAggregateFunction()
	 * @generated
	 * @ordered
	 */
	protected AggregateFunction aggregateFunction;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AggregateColumnFunctionImpl()
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
		return Column_transformation_logicPackage.Literals.AGGREGATE_COLUMN_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AggregateFunction getAggregateFunction()
	{
		return aggregateFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetAggregateFunction(AggregateFunction newAggregateFunction, NotificationChain msgs)
	{
		AggregateFunction oldAggregateFunction = aggregateFunction;
		aggregateFunction = newAggregateFunction;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION, oldAggregateFunction, newAggregateFunction);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAggregateFunction(AggregateFunction newAggregateFunction)
	{
		if (newAggregateFunction != aggregateFunction)
		{
			NotificationChain msgs = null;
			if (aggregateFunction != null)
				msgs = ((InternalEObject)aggregateFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION, null, msgs);
			if (newAggregateFunction != null)
				msgs = ((InternalEObject)newAggregateFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION, null, msgs);
			msgs = basicSetAggregateFunction(newAggregateFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION, newAggregateFunction, newAggregateFunction));
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION:
				return basicSetAggregateFunction(null, msgs);
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION:
				return getAggregateFunction();
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION:
				setAggregateFunction((AggregateFunction)newValue);
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION:
				setAggregateFunction((AggregateFunction)null);
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION:
				return aggregateFunction != null;
		}
		return super.eIsSet(featureID);
	}

} //AggregateColumnFunctionImpl
