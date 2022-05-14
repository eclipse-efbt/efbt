/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.TableCellModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage;

import org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Table Cell Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl.TableCellModuleImpl#getTableCells <em>Table Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TableCellModuleImpl extends ModuleImpl implements TableCellModule {
	/**
	 * The cached value of the '{@link #getTableCells() <em>Table Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTableCells()
	 * @generated
	 * @ordered
	 */
	protected EList<TABLE_CELL> tableCells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TableCellModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return openregspecs_smcubes_extra_extensionPackage.Literals.TABLE_CELL_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TABLE_CELL> getTableCells() {
		if (tableCells == null) {
			tableCells = new EObjectContainmentEList<TABLE_CELL>(TABLE_CELL.class, this, openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS);
		}
		return tableCells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS:
				return ((InternalEList<?>)getTableCells()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS:
				return getTableCells();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS:
				getTableCells().clear();
				getTableCells().addAll((Collection<? extends TABLE_CELL>)newValue);
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS:
				getTableCells().clear();
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE__TABLE_CELLS:
				return tableCells != null && !tableCells.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //TableCellModuleImpl
