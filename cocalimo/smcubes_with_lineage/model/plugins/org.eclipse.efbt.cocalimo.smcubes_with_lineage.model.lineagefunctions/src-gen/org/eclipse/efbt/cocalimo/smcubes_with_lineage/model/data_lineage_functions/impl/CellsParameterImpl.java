/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cells Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.CellsParameterImpl#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CellsParameterImpl extends ParameterImpl implements CellsParameter
{
	/**
	 * The cached value of the '{@link #getCells() <em>Cells</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCells()
	 * @generated
	 * @ordered
	 */
	protected Cell cells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CellsParameterImpl()
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
		return Data_lineage_functionsPackage.Literals.CELLS_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell getCells()
	{
		if (cells != null && cells.eIsProxy()) {
			InternalEObject oldCells = (InternalEObject)cells;
			cells = (Cell)eResolveProxy(oldCells);
			if (cells != oldCells) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS, oldCells, cells));
			}
		}
		return cells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell basicGetCells()
	{
		return cells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCells(Cell newCells)
	{
		Cell oldCells = cells;
		cells = newCells;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS, oldCells, cells));
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
			case Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS:
				if (resolve) return getCells();
				return basicGetCells();
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
			case Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS:
				setCells((Cell)newValue);
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
			case Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS:
				setCells((Cell)null);
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
			case Data_lineage_functionsPackage.CELLS_PARAMETER__CELLS:
				return cells != null;
		}
		return super.eIsSet(featureID);
	}

} //CellsParameterImpl
