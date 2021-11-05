/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportTableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Table Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportTableModuleImpl#getReportTables <em>Report Tables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportTableModuleImpl extends ModuleImpl implements ReportTableModule {
	/**
	 * The cached value of the '{@link #getReportTables() <em>Report Tables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportTables()
	 * @generated
	 * @ordered
	 */
	protected EList<TABLE> reportTables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportTableModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ReportsPackage.Literals.REPORT_TABLE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TABLE> getReportTables() {
		if (reportTables == null) {
			reportTables = new EObjectContainmentEList<TABLE>(TABLE.class, this, ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES);
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
			case ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES:
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
			case ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES:
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
			case ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES:
				getReportTables().clear();
				getReportTables().addAll((Collection<? extends TABLE>)newValue);
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
			case ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES:
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
			case ReportsPackage.REPORT_TABLE_MODULE__REPORT_TABLES:
				return reportTables != null && !reportTables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ReportTableModuleImpl
