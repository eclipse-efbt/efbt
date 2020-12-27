/**
 */
package org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocamo.functions.model.functions.BooleanFunction;

import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.FilterAndGroupToOneRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.Row_transformation_logicPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Filter And Group To One Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.impl.FilterAndGroupToOneRowCreationApproachImpl#getFilterFunction <em>Filter Function</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.impl.FilterAndGroupToOneRowCreationApproachImpl#getGroupByVariables <em>Group By Variables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FilterAndGroupToOneRowCreationApproachImpl extends RowCreationApproachImpl implements FilterAndGroupToOneRowCreationApproach
{
	/**
	 * The cached value of the '{@link #getFilterFunction() <em>Filter Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilterFunction()
	 * @generated
	 * @ordered
	 */
	protected BooleanFunction filterFunction;

	/**
	 * The cached value of the '{@link #getGroupByVariables() <em>Group By Variables</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByVariables()
	 * @generated
	 * @ordered
	 */
	protected EList<VARIABLE> groupByVariables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FilterAndGroupToOneRowCreationApproachImpl()
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
		return Row_transformation_logicPackage.Literals.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BooleanFunction getFilterFunction()
	{
		return filterFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFilterFunction(BooleanFunction newFilterFunction, NotificationChain msgs)
	{
		BooleanFunction oldFilterFunction = filterFunction;
		filterFunction = newFilterFunction;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION, oldFilterFunction, newFilterFunction);
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
	public void setFilterFunction(BooleanFunction newFilterFunction)
	{
		if (newFilterFunction != filterFunction)
		{
			NotificationChain msgs = null;
			if (filterFunction != null)
				msgs = ((InternalEObject)filterFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION, null, msgs);
			if (newFilterFunction != null)
				msgs = ((InternalEObject)newFilterFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION, null, msgs);
			msgs = basicSetFilterFunction(newFilterFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION, newFilterFunction, newFilterFunction));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VARIABLE> getGroupByVariables()
	{
		if (groupByVariables == null)
		{
			groupByVariables = new EObjectResolvingEList<VARIABLE>(VARIABLE.class, this, Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES);
		}
		return groupByVariables;
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
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return basicSetFilterFunction(null, msgs);
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
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return getFilterFunction();
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES:
				return getGroupByVariables();
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
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				setFilterFunction((BooleanFunction)newValue);
				return;
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES:
				getGroupByVariables().clear();
				getGroupByVariables().addAll((Collection<? extends VARIABLE>)newValue);
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
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				setFilterFunction((BooleanFunction)null);
				return;
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES:
				getGroupByVariables().clear();
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
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return filterFunction != null;
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES:
				return groupByVariables != null && !groupByVariables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FilterAndGroupToOneRowCreationApproachImpl
