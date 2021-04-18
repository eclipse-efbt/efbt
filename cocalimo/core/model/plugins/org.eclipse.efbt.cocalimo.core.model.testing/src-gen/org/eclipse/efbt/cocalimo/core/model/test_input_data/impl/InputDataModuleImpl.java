/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Input Data Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.InputDataModuleImpl#getInputData <em>Input Data</em>}</li>
 * </ul>
 *
 * @generated
 */
public class InputDataModuleImpl extends ModuleImpl implements InputDataModule
{
	/**
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected EList<TestInputData> inputData;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected InputDataModuleImpl()
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
		return Test_input_dataPackage.Literals.INPUT_DATA_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TestInputData> getInputData()
	{
		if (inputData == null) {
			inputData = new EObjectContainmentEList<TestInputData>(TestInputData.class, this, Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA);
		}
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA:
				return ((InternalEList<?>)getInputData()).basicRemove(otherEnd, msgs);
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
		switch (featureID) {
			case Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA:
				return getInputData();
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
		switch (featureID) {
			case Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA:
				getInputData().clear();
				getInputData().addAll((Collection<? extends TestInputData>)newValue);
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
			case Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA:
				getInputData().clear();
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
			case Test_input_dataPackage.INPUT_DATA_MODULE__INPUT_DATA:
				return inputData != null && !inputData.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //InputDataModuleImpl
