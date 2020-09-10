/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Cell;
import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cell</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl.CellImpl#getCellID <em>Cell ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl.CellImpl#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CellImpl extends MinimalEObjectImpl.Container implements Cell
{
	/**
	 * The default value of the '{@link #getCellID() <em>Cell ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCellID()
	 * @generated
	 * @ordered
	 */
	protected static final String CELL_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCellID() <em>Cell ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCellID()
	 * @generated
	 * @ordered
	 */
	protected String cellID = CELL_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getColumn() <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumn()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE column;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CellImpl()
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
		return Column_structured_dataPackage.Literals.CELL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCellID()
	{
		return cellID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCellID(String newCellID)
	{
		String oldCellID = cellID;
		cellID = newCellID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_structured_dataPackage.CELL__CELL_ID, oldCellID, cellID));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getColumn()
	{
		if (column != null && column.eIsProxy())
		{
			InternalEObject oldColumn = (InternalEObject)column;
			column = (VARIABLE)eResolveProxy(oldColumn);
			if (column != oldColumn)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Column_structured_dataPackage.CELL__COLUMN, oldColumn, column));
			}
		}
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetColumn()
	{
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setColumn(VARIABLE newColumn)
	{
		VARIABLE oldColumn = column;
		column = newColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_structured_dataPackage.CELL__COLUMN, oldColumn, column));
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
			case Column_structured_dataPackage.CELL__CELL_ID:
				return getCellID();
			case Column_structured_dataPackage.CELL__COLUMN:
				if (resolve) return getColumn();
				return basicGetColumn();
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
			case Column_structured_dataPackage.CELL__CELL_ID:
				setCellID((String)newValue);
				return;
			case Column_structured_dataPackage.CELL__COLUMN:
				setColumn((VARIABLE)newValue);
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
			case Column_structured_dataPackage.CELL__CELL_ID:
				setCellID(CELL_ID_EDEFAULT);
				return;
			case Column_structured_dataPackage.CELL__COLUMN:
				setColumn((VARIABLE)null);
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
			case Column_structured_dataPackage.CELL__CELL_ID:
				return CELL_ID_EDEFAULT == null ? cellID != null : !CELL_ID_EDEFAULT.equals(cellID);
			case Column_structured_dataPackage.CELL__COLUMN:
				return column != null;
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
		result.append(" (cellID: ");
		result.append(cellID);
		result.append(')');
		return result.toString();
	}

} //CellImpl
