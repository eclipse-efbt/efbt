/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Populated Report</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.PopulatedReportImpl#getReportCellValues <em>Report Cell Values</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.PopulatedReportImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class PopulatedReportImpl extends MinimalEObjectImpl.Container implements PopulatedReport
{
	/**
	 * The cached value of the '{@link #getReportCellValues() <em>Report Cell Values</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCellValues()
	 * @generated
	 * @ordered
	 */
	protected ReportCellValue reportCellValues;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PopulatedReportImpl()
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
		return ReportsPackage.Literals.POPULATED_REPORT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellValue getReportCellValues()
	{
		return reportCellValues;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetReportCellValues(ReportCellValue newReportCellValues, NotificationChain msgs)
	{
		ReportCellValue oldReportCellValues = reportCellValues;
		reportCellValues = newReportCellValues;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES, oldReportCellValues, newReportCellValues);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setReportCellValues(ReportCellValue newReportCellValues)
	{
		if (newReportCellValues != reportCellValues) {
			NotificationChain msgs = null;
			if (reportCellValues != null)
				msgs = ((InternalEObject)reportCellValues).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES, null, msgs);
			if (newReportCellValues != null)
				msgs = ((InternalEObject)newReportCellValues).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES, null, msgs);
			msgs = basicSetReportCellValues(newReportCellValues, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES, newReportCellValues, newReportCellValues));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.POPULATED_REPORT__NAME, oldName, name));
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
			case ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES:
				return basicSetReportCellValues(null, msgs);
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
			case ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES:
				return getReportCellValues();
			case ReportsPackage.POPULATED_REPORT__NAME:
				return getName();
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
			case ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES:
				setReportCellValues((ReportCellValue)newValue);
				return;
			case ReportsPackage.POPULATED_REPORT__NAME:
				setName((String)newValue);
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
			case ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES:
				setReportCellValues((ReportCellValue)null);
				return;
			case ReportsPackage.POPULATED_REPORT__NAME:
				setName(NAME_EDEFAULT);
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
			case ReportsPackage.POPULATED_REPORT__REPORT_CELL_VALUES:
				return reportCellValues != null;
			case ReportsPackage.POPULATED_REPORT__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //PopulatedReportImpl
