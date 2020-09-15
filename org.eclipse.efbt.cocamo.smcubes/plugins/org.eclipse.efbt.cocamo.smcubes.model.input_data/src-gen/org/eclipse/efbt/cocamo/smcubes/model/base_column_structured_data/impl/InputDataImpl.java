/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Input Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.impl.InputDataImpl#getSourceTableData <em>Source Table Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.impl.InputDataImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class InputDataImpl extends MinimalEObjectImpl.Container implements InputData
{
	/**
	 * The cached value of the '{@link #getSourceTableData() <em>Source Table Data</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceTableData()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseColumnStructuredData> sourceTableData;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected InputDataImpl()
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
		return Base_column_structured_dataPackage.Literals.INPUT_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BaseColumnStructuredData> getSourceTableData()
	{
		if (sourceTableData == null)
		{
			sourceTableData = new EObjectContainmentEList<BaseColumnStructuredData>(BaseColumnStructuredData.class, this, Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA);
		}
		return sourceTableData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Base_column_structured_dataPackage.INPUT_DATA__NAME, oldName, name));
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
			case Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA:
				return ((InternalEList<?>)getSourceTableData()).basicRemove(otherEnd, msgs);
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
			case Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA:
				return getSourceTableData();
			case Base_column_structured_dataPackage.INPUT_DATA__NAME:
				return getName();
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
			case Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA:
				getSourceTableData().clear();
				getSourceTableData().addAll((Collection<? extends BaseColumnStructuredData>)newValue);
				return;
			case Base_column_structured_dataPackage.INPUT_DATA__NAME:
				setName((String)newValue);
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
			case Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA:
				getSourceTableData().clear();
				return;
			case Base_column_structured_dataPackage.INPUT_DATA__NAME:
				setName(NAME_EDEFAULT);
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
			case Base_column_structured_dataPackage.INPUT_DATA__SOURCE_TABLE_DATA:
				return sourceTableData != null && !sourceTableData.isEmpty();
			case Base_column_structured_dataPackage.INPUT_DATA__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //InputDataImpl
