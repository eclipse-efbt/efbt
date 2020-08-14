/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl;

import org.eclipse.efbt.data_structures.smcubes.model.reports.ReportCell;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SQLViewImpl;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Cell View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewImpl#getReportCell <em>Report Cell</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellViewImpl extends SQLViewImpl implements ReportCellView
{
	/**
	 * The cached value of the '{@link #getReportCell() <em>Report Cell</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCell()
	 * @generated
	 * @ordered
	 */
	protected ReportCell reportCell;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellViewImpl()
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
		return Trl_report_cell_viewsPackage.Literals.REPORT_CELL_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell getReportCell()
	{
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetReportCell(ReportCell newReportCell, NotificationChain msgs)
	{
		ReportCell oldReportCell = reportCell;
		reportCell = newReportCell;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, oldReportCell, newReportCell);
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
	public void setReportCell(ReportCell newReportCell)
	{
		if (newReportCell != reportCell)
		{
			NotificationChain msgs = null;
			if (reportCell != null)
				msgs = ((InternalEObject)reportCell).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, null, msgs);
			if (newReportCell != null)
				msgs = ((InternalEObject)newReportCell).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, null, msgs);
			msgs = basicSetReportCell(newReportCell, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, newReportCell, newReportCell));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				return basicSetReportCell(null, msgs);
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
		switch (featureID)
		{
			case Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				return getReportCell();
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
			case Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				setReportCell((ReportCell)newValue);
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
			case Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				setReportCell((ReportCell)null);
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
			case Trl_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				return reportCell != null;
		}
		return super.eIsSet(featureID);
	}

} //ReportCellViewImpl
