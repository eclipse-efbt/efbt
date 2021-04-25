/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test.impl.ResultImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes Report Results</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesReportResultsImpl#getReportCells <em>Report Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesReportResultsImpl extends ResultImpl implements SMCubesReportResults {
	/**
	 * The cached value of the '{@link #getReportCells() <em>Report Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCells()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellValue> reportCells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesReportResultsImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ProgramPackage.Literals.SM_CUBES_REPORT_RESULTS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReportCellValue> getReportCells() {
		if (reportCells == null) {
			reportCells = new EObjectContainmentEList<ReportCellValue>(ReportCellValue.class, this, ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS);
		}
		return reportCells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS:
				return ((InternalEList<?>)getReportCells()).basicRemove(otherEnd, msgs);
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
			case ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS:
				return getReportCells();
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
			case ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS:
				getReportCells().clear();
				getReportCells().addAll((Collection<? extends ReportCellValue>)newValue);
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
			case ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS:
				getReportCells().clear();
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
			case ProgramPackage.SM_CUBES_REPORT_RESULTS__REPORT_CELLS:
				return reportCells != null && !reportCells.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SMCubesReportResultsImpl
