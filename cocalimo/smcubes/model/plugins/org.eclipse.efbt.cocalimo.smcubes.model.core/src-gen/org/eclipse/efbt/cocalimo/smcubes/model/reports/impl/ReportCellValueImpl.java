/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Cell Value</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportCellValueImpl#getReportCell <em>Report Cell</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportCellValueImpl#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellValueImpl extends MinimalEObjectImpl.Container implements ReportCellValue {
	/**
	 * The cached value of the '{@link #getReportCell() <em>Report Cell</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCell()
	 * @generated
	 * @ordered
	 */
	protected ReportCell reportCell;

	/**
	 * The default value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected static final String VALUE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected String value = VALUE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellValueImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ReportsPackage.Literals.REPORT_CELL_VALUE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell getReportCell() {
		if (reportCell != null && reportCell.eIsProxy()) {
			InternalEObject oldReportCell = (InternalEObject)reportCell;
			reportCell = (ReportCell)eResolveProxy(oldReportCell);
			if (reportCell != oldReportCell) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL, oldReportCell, reportCell));
			}
		}
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportCell basicGetReportCell() {
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setReportCell(ReportCell newReportCell) {
		ReportCell oldReportCell = reportCell;
		reportCell = newReportCell;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL, oldReportCell, reportCell));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getValue() {
		return value;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValue(String newValue) {
		String oldValue = value;
		value = newValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.REPORT_CELL_VALUE__VALUE, oldValue, value));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL:
				if (resolve) return getReportCell();
				return basicGetReportCell();
			case ReportsPackage.REPORT_CELL_VALUE__VALUE:
				return getValue();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL:
				setReportCell((ReportCell)newValue);
				return;
			case ReportsPackage.REPORT_CELL_VALUE__VALUE:
				setValue((String)newValue);
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
			case ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL:
				setReportCell((ReportCell)null);
				return;
			case ReportsPackage.REPORT_CELL_VALUE__VALUE:
				setValue(VALUE_EDEFAULT);
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
			case ReportsPackage.REPORT_CELL_VALUE__REPORT_CELL:
				return reportCell != null;
			case ReportsPackage.REPORT_CELL_VALUE__VALUE:
				return VALUE_EDEFAULT == null ? value != null : !VALUE_EDEFAULT.equals(value);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (value: ");
		result.append(value);
		result.append(')');
		return result.toString();
	}

} //ReportCellValueImpl
