/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.impl;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.ColumnSettingCondition;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Column Setting Condition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.ColumnSettingConditionImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.ColumnSettingConditionImpl#getVariable <em>Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.ColumnSettingConditionImpl#getFilterConditions <em>Filter Conditions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ColumnSettingConditionImpl extends CodeConditionImpl implements ColumnSettingCondition
{
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
	 * The cached value of the '{@link #getVariable() <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable;

	/**
	 * The cached value of the '{@link #getFilterConditions() <em>Filter Conditions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilterConditions()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube filterConditions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ColumnSettingConditionImpl()
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
		return Domain_model_mappingPackage.Literals.COLUMN_SETTING_CONDITION;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE, oldCube, cube));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE, oldCube, cube));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE, oldVariable, variable));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE, oldVariable, variable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getFilterConditions()
	{
		return filterConditions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFilterConditions(FreeBirdToolsCube newFilterConditions, NotificationChain msgs)
	{
		FreeBirdToolsCube oldFilterConditions = filterConditions;
		filterConditions = newFilterConditions;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS, oldFilterConditions, newFilterConditions);
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
	public void setFilterConditions(FreeBirdToolsCube newFilterConditions)
	{
		if (newFilterConditions != filterConditions)
		{
			NotificationChain msgs = null;
			if (filterConditions != null)
				msgs = ((InternalEObject)filterConditions).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS, null, msgs);
			if (newFilterConditions != null)
				msgs = ((InternalEObject)newFilterConditions).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS, null, msgs);
			msgs = basicSetFilterConditions(newFilterConditions, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS, newFilterConditions, newFilterConditions));
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
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS:
				return basicSetFilterConditions(null, msgs);
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
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE:
				if (resolve) return getVariable();
				return basicGetVariable();
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS:
				return getFilterConditions();
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
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE:
				setCube((FreeBirdToolsCube)newValue);
				return;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE:
				setVariable((VARIABLE)newValue);
				return;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS:
				setFilterConditions((FreeBirdToolsCube)newValue);
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
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE:
				setCube((FreeBirdToolsCube)null);
				return;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE:
				setVariable((VARIABLE)null);
				return;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS:
				setFilterConditions((FreeBirdToolsCube)null);
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
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__CUBE:
				return cube != null;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__VARIABLE:
				return variable != null;
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION__FILTER_CONDITIONS:
				return filterConditions != null;
		}
		return super.eIsSet(featureID);
	}

} //ColumnSettingConditionImpl
