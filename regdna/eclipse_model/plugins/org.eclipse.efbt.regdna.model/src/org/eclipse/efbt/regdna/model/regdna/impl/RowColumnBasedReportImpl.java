/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.ColumnFilters;
import org.eclipse.efbt.regdna.model.regdna.RowColumnBasedReport;
import org.eclipse.efbt.regdna.model.regdna.RowFilters;
import org.eclipse.efbt.regdna.model.regdna.WholeReportFilters;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Column Based Report</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RowColumnBasedReportImpl#getColumnFilters <em>Column Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RowColumnBasedReportImpl#getRowFilters <em>Row Filters</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RowColumnBasedReportImpl#getWholeReportFilters <em>Whole Report Filters</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowColumnBasedReportImpl extends ReportImpl implements RowColumnBasedReport {
	/**
	 * The cached value of the '{@link #getColumnFilters() <em>Column Filters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnFilters()
	 * @generated
	 * @ordered
	 */
	protected EList<ColumnFilters> columnFilters;

	/**
	 * The cached value of the '{@link #getRowFilters() <em>Row Filters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowFilters()
	 * @generated
	 * @ordered
	 */
	protected EList<RowFilters> rowFilters;

	/**
	 * The cached value of the '{@link #getWholeReportFilters() <em>Whole Report Filters</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWholeReportFilters()
	 * @generated
	 * @ordered
	 */
	protected WholeReportFilters wholeReportFilters;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowColumnBasedReportImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getRowColumnBasedReport();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ColumnFilters> getColumnFilters() {
		if (columnFilters == null) {
			columnFilters = new EObjectContainmentEList<ColumnFilters>(ColumnFilters.class, this, regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS);
		}
		return columnFilters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RowFilters> getRowFilters() {
		if (rowFilters == null) {
			rowFilters = new EObjectContainmentEList<RowFilters>(RowFilters.class, this, regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS);
		}
		return rowFilters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public WholeReportFilters getWholeReportFilters() {
		return wholeReportFilters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWholeReportFilters(WholeReportFilters newWholeReportFilters, NotificationChain msgs) {
		WholeReportFilters oldWholeReportFilters = wholeReportFilters;
		wholeReportFilters = newWholeReportFilters;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS, oldWholeReportFilters, newWholeReportFilters);
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
	public void setWholeReportFilters(WholeReportFilters newWholeReportFilters) {
		if (newWholeReportFilters != wholeReportFilters) {
			NotificationChain msgs = null;
			if (wholeReportFilters != null)
				msgs = ((InternalEObject)wholeReportFilters).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS, null, msgs);
			if (newWholeReportFilters != null)
				msgs = ((InternalEObject)newWholeReportFilters).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS, null, msgs);
			msgs = basicSetWholeReportFilters(newWholeReportFilters, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS, newWholeReportFilters, newWholeReportFilters));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS:
				return ((InternalEList<?>)getColumnFilters()).basicRemove(otherEnd, msgs);
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS:
				return ((InternalEList<?>)getRowFilters()).basicRemove(otherEnd, msgs);
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS:
				return basicSetWholeReportFilters(null, msgs);
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
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS:
				return getColumnFilters();
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS:
				return getRowFilters();
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS:
				return getWholeReportFilters();
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
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS:
				getColumnFilters().clear();
				getColumnFilters().addAll((Collection<? extends ColumnFilters>)newValue);
				return;
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS:
				getRowFilters().clear();
				getRowFilters().addAll((Collection<? extends RowFilters>)newValue);
				return;
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS:
				setWholeReportFilters((WholeReportFilters)newValue);
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
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS:
				getColumnFilters().clear();
				return;
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS:
				getRowFilters().clear();
				return;
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS:
				setWholeReportFilters((WholeReportFilters)null);
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
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__COLUMN_FILTERS:
				return columnFilters != null && !columnFilters.isEmpty();
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__ROW_FILTERS:
				return rowFilters != null && !rowFilters.isEmpty();
			case regdnaPackage.ROW_COLUMN_BASED_REPORT__WHOLE_REPORT_FILTERS:
				return wholeReportFilters != null;
		}
		return super.eIsSet(featureID);
	}

} //RowColumnBasedReportImpl
