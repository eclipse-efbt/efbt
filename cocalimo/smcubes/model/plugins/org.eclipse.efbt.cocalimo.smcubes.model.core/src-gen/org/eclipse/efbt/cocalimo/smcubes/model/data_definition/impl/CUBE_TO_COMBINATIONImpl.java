/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE TO COMBINATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl#getCombination_id <em>Combination id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl#getCube_id <em>Cube id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_TO_COMBINATIONImpl extends MinimalEObjectImpl.Container implements CUBE_TO_COMBINATION
{
	/**
	 * The cached value of the '{@link #getCombination_id() <em>Combination id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombination_id()
	 * @generated
	 * @ordered
	 */
	protected COMBINATION combination_id;

	/**
	 * The cached value of the '{@link #getCube_id() <em>Cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_TO_COMBINATIONImpl()
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
		return Data_definitionPackage.Literals.CUBE_TO_COMBINATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public COMBINATION getCombination_id()
	{
		if (combination_id != null && combination_id.eIsProxy()) {
			InternalEObject oldCombination_id = (InternalEObject)combination_id;
			combination_id = (COMBINATION)eResolveProxy(oldCombination_id);
			if (combination_id != oldCombination_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID, oldCombination_id, combination_id));
			}
		}
		return combination_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public COMBINATION basicGetCombination_id()
	{
		return combination_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCombination_id(COMBINATION newCombination_id)
	{
		COMBINATION oldCombination_id = combination_id;
		combination_id = newCombination_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID, oldCombination_id, combination_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getCube_id()
	{
		if (cube_id != null && cube_id.eIsProxy()) {
			InternalEObject oldCube_id = (InternalEObject)cube_id;
			cube_id = (CUBE)eResolveProxy(oldCube_id);
			if (cube_id != oldCube_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID, oldCube_id, cube_id));
			}
		}
		return cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube_id()
	{
		return cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_id(CUBE newCube_id)
	{
		CUBE oldCube_id = cube_id;
		cube_id = newCube_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID, oldCube_id, cube_id));
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
			case Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID:
				if (resolve) return getCombination_id();
				return basicGetCombination_id();
			case Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID:
				if (resolve) return getCube_id();
				return basicGetCube_id();
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
			case Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID:
				setCombination_id((COMBINATION)newValue);
				return;
			case Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID:
				setCube_id((CUBE)newValue);
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
			case Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID:
				setCombination_id((COMBINATION)null);
				return;
			case Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID:
				setCube_id((CUBE)null);
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
			case Data_definitionPackage.CUBE_TO_COMBINATION__COMBINATION_ID:
				return combination_id != null;
			case Data_definitionPackage.CUBE_TO_COMBINATION__CUBE_ID:
				return cube_id != null;
		}
		return super.eIsSet(featureID);
	}

} //CUBE_TO_COMBINATIONImpl
