/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage;
import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.RowData;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl.RowDataImpl#getRowID <em>Row ID</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowDataImpl extends MinimalEObjectImpl.Container implements RowData
{
	/**
	 * The default value of the '{@link #getRowID() <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowID()
	 * @generated
	 * @ordered
	 */
	protected static final String ROW_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getRowID() <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowID()
	 * @generated
	 * @ordered
	 */
	protected String rowID = ROW_ID_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowDataImpl()
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
		return Column_structured_dataPackage.Literals.ROW_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getRowID()
	{
		return rowID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRowID(String newRowID)
	{
		String oldRowID = rowID;
		rowID = newRowID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_structured_dataPackage.ROW_DATA__ROW_ID, oldRowID, rowID));
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
			case Column_structured_dataPackage.ROW_DATA__ROW_ID:
				return getRowID();
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
			case Column_structured_dataPackage.ROW_DATA__ROW_ID:
				setRowID((String)newValue);
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
			case Column_structured_dataPackage.ROW_DATA__ROW_ID:
				setRowID(ROW_ID_EDEFAULT);
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
			case Column_structured_dataPackage.ROW_DATA__ROW_ID:
				return ROW_ID_EDEFAULT == null ? rowID != null : !ROW_ID_EDEFAULT.equals(rowID);
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
		result.append(" (rowID: ");
		result.append(rowID);
		result.append(')');
		return result.toString();
	}

} //RowDataImpl
