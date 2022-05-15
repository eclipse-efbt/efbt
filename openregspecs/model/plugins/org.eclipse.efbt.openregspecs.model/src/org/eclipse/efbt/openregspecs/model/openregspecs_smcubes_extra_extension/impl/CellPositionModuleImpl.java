/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.CellPositionModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage;

import org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cell Position Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl.CellPositionModuleImpl#getReportTables <em>Report Tables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CellPositionModuleImpl extends ModuleImpl implements CellPositionModule {
	/**
	 * The cached value of the '{@link #getReportTables() <em>Report Tables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportTables()
	 * @generated
	 * @ordered
	 */
	protected EList<CELL_POSITION> reportTables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CellPositionModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return openregspecs_smcubes_extra_extensionPackage.Literals.CELL_POSITION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<CELL_POSITION> getReportTables() {
		if (reportTables == null) {
			reportTables = new EObjectContainmentEList<CELL_POSITION>(CELL_POSITION.class, this, openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES);
		}
		return reportTables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES:
				return ((InternalEList<?>)getReportTables()).basicRemove(otherEnd, msgs);
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
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES:
				return getReportTables();
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
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES:
				getReportTables().clear();
				getReportTables().addAll((Collection<? extends CELL_POSITION>)newValue);
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
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES:
				getReportTables().clear();
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
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE__REPORT_TABLES:
				return reportTables != null && !reportTables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CellPositionModuleImpl
