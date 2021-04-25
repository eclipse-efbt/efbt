/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Column Structured Data Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataModuleImpl#getCubeData <em>Cube Data</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ColumnStructuredDataModuleImpl extends ModuleImpl implements ColumnStructuredDataModule
{
	/**
	 * The cached value of the '{@link #getCubeData() <em>Cube Data</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeData()
	 * @generated
	 * @ordered
	 */
	protected EList<ColumnStructuredData> cubeData;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ColumnStructuredDataModuleImpl()
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
		return Column_structured_dataPackage.Literals.COLUMN_STRUCTURED_DATA_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ColumnStructuredData> getCubeData()
	{
		if (cubeData == null) {
			cubeData = new EObjectContainmentEList<ColumnStructuredData>(ColumnStructuredData.class, this, Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA);
		}
		return cubeData;
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
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA:
				return ((InternalEList<?>)getCubeData()).basicRemove(otherEnd, msgs);
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
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA:
				return getCubeData();
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
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA:
				getCubeData().clear();
				getCubeData().addAll((Collection<? extends ColumnStructuredData>)newValue);
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
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA:
				getCubeData().clear();
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
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA:
				return cubeData != null && !cubeData.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ColumnStructuredDataModuleImpl
