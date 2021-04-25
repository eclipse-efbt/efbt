/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_views.impl.DTMViewImpl;

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
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.ReportCellViewImpl#getReportCell <em>Report Cell</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.ReportCellViewImpl#getStructColumnHoldingDimension <em>Struct Column Holding Dimension</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.ReportCellViewImpl#getStructColumnHoldingMeasure <em>Struct Column Holding Measure</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellViewImpl extends DTMViewImpl implements ReportCellView {
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
	 * The cached value of the '{@link #getStructColumnHoldingDimension() <em>Struct Column Holding Dimension</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructColumnHoldingDimension()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable structColumnHoldingDimension;

	/**
	 * The cached value of the '{@link #getStructColumnHoldingMeasure() <em>Struct Column Holding Measure</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructColumnHoldingMeasure()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable structColumnHoldingMeasure;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellViewImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_report_cell_viewsPackage.Literals.REPORT_CELL_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell getReportCell() {
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetReportCell(ReportCell newReportCell, NotificationChain msgs) {
		ReportCell oldReportCell = reportCell;
		reportCell = newReportCell;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, oldReportCell, newReportCell);
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
	public void setReportCell(ReportCell newReportCell) {
		if (newReportCell != reportCell) {
			NotificationChain msgs = null;
			if (reportCell != null)
				msgs = ((InternalEObject)reportCell).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, null, msgs);
			if (newReportCell != null)
				msgs = ((InternalEObject)newReportCell).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, null, msgs);
			msgs = basicSetReportCell(newReportCell, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL, newReportCell, newReportCell));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getStructColumnHoldingDimension() {
		if (structColumnHoldingDimension != null && structColumnHoldingDimension.eIsProxy()) {
			InternalEObject oldStructColumnHoldingDimension = (InternalEObject)structColumnHoldingDimension;
			structColumnHoldingDimension = (StructTypedVariable)eResolveProxy(oldStructColumnHoldingDimension);
			if (structColumnHoldingDimension != oldStructColumnHoldingDimension) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION, oldStructColumnHoldingDimension, structColumnHoldingDimension));
			}
		}
		return structColumnHoldingDimension;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetStructColumnHoldingDimension() {
		return structColumnHoldingDimension;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStructColumnHoldingDimension(StructTypedVariable newStructColumnHoldingDimension) {
		StructTypedVariable oldStructColumnHoldingDimension = structColumnHoldingDimension;
		structColumnHoldingDimension = newStructColumnHoldingDimension;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION, oldStructColumnHoldingDimension, structColumnHoldingDimension));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getStructColumnHoldingMeasure() {
		if (structColumnHoldingMeasure != null && structColumnHoldingMeasure.eIsProxy()) {
			InternalEObject oldStructColumnHoldingMeasure = (InternalEObject)structColumnHoldingMeasure;
			structColumnHoldingMeasure = (StructTypedVariable)eResolveProxy(oldStructColumnHoldingMeasure);
			if (structColumnHoldingMeasure != oldStructColumnHoldingMeasure) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE, oldStructColumnHoldingMeasure, structColumnHoldingMeasure));
			}
		}
		return structColumnHoldingMeasure;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetStructColumnHoldingMeasure() {
		return structColumnHoldingMeasure;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStructColumnHoldingMeasure(StructTypedVariable newStructColumnHoldingMeasure) {
		StructTypedVariable oldStructColumnHoldingMeasure = structColumnHoldingMeasure;
		structColumnHoldingMeasure = newStructColumnHoldingMeasure;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE, oldStructColumnHoldingMeasure, structColumnHoldingMeasure));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				return getReportCell();
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION:
				if (resolve) return getStructColumnHoldingDimension();
				return basicGetStructColumnHoldingDimension();
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE:
				if (resolve) return getStructColumnHoldingMeasure();
				return basicGetStructColumnHoldingMeasure();
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				setReportCell((ReportCell)newValue);
				return;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION:
				setStructColumnHoldingDimension((StructTypedVariable)newValue);
				return;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE:
				setStructColumnHoldingMeasure((StructTypedVariable)newValue);
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				setReportCell((ReportCell)null);
				return;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION:
				setStructColumnHoldingDimension((StructTypedVariable)null);
				return;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE:
				setStructColumnHoldingMeasure((StructTypedVariable)null);
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__REPORT_CELL:
				return reportCell != null;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION:
				return structColumnHoldingDimension != null;
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE:
				return structColumnHoldingMeasure != null;
		}
		return super.eIsSet(featureID);
	}

} //ReportCellViewImpl
