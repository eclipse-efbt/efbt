/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCell;
import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Row Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseRowDataImpl#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseRowDataImpl extends RowDataImpl implements BaseRowData
{
	/**
	 * The cached value of the '{@link #getCells() <em>Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCells()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseCell> cells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseRowDataImpl()
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
		return Base_column_structured_dataPackage.Literals.BASE_ROW_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BaseCell> getCells()
	{
		if (cells == null) {
			cells = new EObjectContainmentEList<BaseCell>(BaseCell.class, this, Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS);
		}
		return cells;
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
			case Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS:
				return ((InternalEList<?>)getCells()).basicRemove(otherEnd, msgs);
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
			case Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS:
				return getCells();
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
			case Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS:
				getCells().clear();
				getCells().addAll((Collection<? extends BaseCell>)newValue);
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
			case Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS:
				getCells().clear();
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
			case Base_column_structured_dataPackage.BASE_ROW_DATA__CELLS:
				return cells != null && !cells.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BaseRowDataImpl
