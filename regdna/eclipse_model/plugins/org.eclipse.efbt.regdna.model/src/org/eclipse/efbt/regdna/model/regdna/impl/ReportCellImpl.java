/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.Filter;
import org.eclipse.efbt.regdna.model.regdna.ReportCell;
import org.eclipse.efbt.regdna.model.regdna.ReportColumn;
import org.eclipse.efbt.regdna.model.regdna.ReportRow;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Cell</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl#getRow <em>Row</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl#getFilters <em>Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl#getMetric <em>Metric</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl#getDatapointID <em>Datapoint ID</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellImpl extends MinimalEObjectImpl.Container implements ReportCell {
	/**
	 * The cached value of the '{@link #getRow() <em>Row</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRow()
	 * @generated
	 * @ordered
	 */
	protected ReportRow row;

	/**
	 * The cached value of the '{@link #getColumn() <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumn()
	 * @generated
	 * @ordered
	 */
	protected ReportColumn column;

	/**
	 * The cached value of the '{@link #getFilters() <em>Filters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilters()
	 * @generated
	 * @ordered
	 */
	protected EList<Filter> filters;

	/**
	 * The cached value of the '{@link #getMetric() <em>Metric</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMetric()
	 * @generated
	 * @ordered
	 */
	protected ELAttribute metric;

	/**
	 * The default value of the '{@link #getDatapointID() <em>Datapoint ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDatapointID()
	 * @generated
	 * @ordered
	 */
	protected static final String DATAPOINT_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDatapointID() <em>Datapoint ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDatapointID()
	 * @generated
	 * @ordered
	 */
	protected String datapointID = DATAPOINT_ID_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getReportCell();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportRow getRow() {
		if (row != null && row.eIsProxy()) {
			InternalEObject oldRow = (InternalEObject)row;
			row = (ReportRow)eResolveProxy(oldRow);
			if (row != oldRow) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.REPORT_CELL__ROW, oldRow, row));
			}
		}
		return row;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportRow basicGetRow() {
		return row;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setRow(ReportRow newRow) {
		ReportRow oldRow = row;
		row = newRow;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.REPORT_CELL__ROW, oldRow, row));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportColumn getColumn() {
		if (column != null && column.eIsProxy()) {
			InternalEObject oldColumn = (InternalEObject)column;
			column = (ReportColumn)eResolveProxy(oldColumn);
			if (column != oldColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.REPORT_CELL__COLUMN, oldColumn, column));
			}
		}
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportColumn basicGetColumn() {
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumn(ReportColumn newColumn) {
		ReportColumn oldColumn = column;
		column = newColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.REPORT_CELL__COLUMN, oldColumn, column));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Filter> getFilters() {
		if (filters == null) {
			filters = new EObjectContainmentEList<Filter>(Filter.class, this, regdnaPackage.REPORT_CELL__FILTERS);
		}
		return filters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAttribute getMetric() {
		if (metric != null && metric.eIsProxy()) {
			InternalEObject oldMetric = (InternalEObject)metric;
			metric = (ELAttribute)eResolveProxy(oldMetric);
			if (metric != oldMetric) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.REPORT_CELL__METRIC, oldMetric, metric));
			}
		}
		return metric;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAttribute basicGetMetric() {
		return metric;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMetric(ELAttribute newMetric) {
		ELAttribute oldMetric = metric;
		metric = newMetric;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.REPORT_CELL__METRIC, oldMetric, metric));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDatapointID() {
		return datapointID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDatapointID(String newDatapointID) {
		String oldDatapointID = datapointID;
		datapointID = newDatapointID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.REPORT_CELL__DATAPOINT_ID, oldDatapointID, datapointID));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.REPORT_CELL__FILTERS:
				return ((InternalEList<?>)getFilters()).basicRemove(otherEnd, msgs);
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
			case regdnaPackage.REPORT_CELL__ROW:
				if (resolve) return getRow();
				return basicGetRow();
			case regdnaPackage.REPORT_CELL__COLUMN:
				if (resolve) return getColumn();
				return basicGetColumn();
			case regdnaPackage.REPORT_CELL__FILTERS:
				return getFilters();
			case regdnaPackage.REPORT_CELL__METRIC:
				if (resolve) return getMetric();
				return basicGetMetric();
			case regdnaPackage.REPORT_CELL__DATAPOINT_ID:
				return getDatapointID();
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
			case regdnaPackage.REPORT_CELL__ROW:
				setRow((ReportRow)newValue);
				return;
			case regdnaPackage.REPORT_CELL__COLUMN:
				setColumn((ReportColumn)newValue);
				return;
			case regdnaPackage.REPORT_CELL__FILTERS:
				getFilters().clear();
				getFilters().addAll((Collection<? extends Filter>)newValue);
				return;
			case regdnaPackage.REPORT_CELL__METRIC:
				setMetric((ELAttribute)newValue);
				return;
			case regdnaPackage.REPORT_CELL__DATAPOINT_ID:
				setDatapointID((String)newValue);
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
			case regdnaPackage.REPORT_CELL__ROW:
				setRow((ReportRow)null);
				return;
			case regdnaPackage.REPORT_CELL__COLUMN:
				setColumn((ReportColumn)null);
				return;
			case regdnaPackage.REPORT_CELL__FILTERS:
				getFilters().clear();
				return;
			case regdnaPackage.REPORT_CELL__METRIC:
				setMetric((ELAttribute)null);
				return;
			case regdnaPackage.REPORT_CELL__DATAPOINT_ID:
				setDatapointID(DATAPOINT_ID_EDEFAULT);
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
			case regdnaPackage.REPORT_CELL__ROW:
				return row != null;
			case regdnaPackage.REPORT_CELL__COLUMN:
				return column != null;
			case regdnaPackage.REPORT_CELL__FILTERS:
				return filters != null && !filters.isEmpty();
			case regdnaPackage.REPORT_CELL__METRIC:
				return metric != null;
			case regdnaPackage.REPORT_CELL__DATAPOINT_ID:
				return DATAPOINT_ID_EDEFAULT == null ? datapointID != null : !DATAPOINT_ID_EDEFAULT.equals(datapointID);
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
		result.append(" (datapointID: ");
		result.append(datapointID);
		result.append(')');
		return result.toString();
	}

} //ReportCellImpl
