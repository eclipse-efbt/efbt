/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;

import org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Results</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.ReportResultsImpl#getReportResults <em>Report Results</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportResultsImpl extends MinimalEObjectImpl.Container implements ReportResults {
	/**
	 * The cached value of the '{@link #getReportResults() <em>Report Results</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportResults()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellValue> reportResults;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportResultsImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.REPORT_RESULTS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReportCellValue> getReportResults() {
		if (reportResults == null) {
			reportResults = new EObjectContainmentEList<ReportCellValue>(ReportCellValue.class, this, TestingPackage.REPORT_RESULTS__REPORT_RESULTS);
		}
		return reportResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.REPORT_RESULTS__REPORT_RESULTS:
				return ((InternalEList<?>)getReportResults()).basicRemove(otherEnd, msgs);
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
			case TestingPackage.REPORT_RESULTS__REPORT_RESULTS:
				return getReportResults();
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
			case TestingPackage.REPORT_RESULTS__REPORT_RESULTS:
				getReportResults().clear();
				getReportResults().addAll((Collection<? extends ReportCellValue>)newValue);
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
			case TestingPackage.REPORT_RESULTS__REPORT_RESULTS:
				getReportResults().clear();
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
			case TestingPackage.REPORT_RESULTS__REPORT_RESULTS:
				return reportResults != null && !reportResults.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ReportResultsImpl
