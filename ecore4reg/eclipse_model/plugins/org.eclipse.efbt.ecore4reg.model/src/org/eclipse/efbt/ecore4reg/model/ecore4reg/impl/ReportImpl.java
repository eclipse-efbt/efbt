/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Report;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ReportCell;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ReportColumn;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ReportRow;

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
 * An implementation of the model object '<em><b>Report</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ReportImpl#getOutputLayer <em>Output Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ReportImpl#getRows <em>Rows</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ReportImpl#getColumns <em>Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ReportImpl#getReportCells <em>Report Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportImpl extends MinimalEObjectImpl.Container implements Report {
	/**
	 * The cached value of the '{@link #getOutputLayer() <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputLayer()
	 * @generated
	 * @ordered
	 */
	protected ELClass outputLayer;

	/**
	 * The cached value of the '{@link #getRows() <em>Rows</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRows()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportRow> rows;

	/**
	 * The cached value of the '{@link #getColumns() <em>Columns</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportColumn> columns;

	/**
	 * The cached value of the '{@link #getReportCells() <em>Report Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCells()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCell> reportCells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getReport();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass getOutputLayer() {
		if (outputLayer != null && outputLayer.eIsProxy()) {
			InternalEObject oldOutputLayer = (InternalEObject)outputLayer;
			outputLayer = (ELClass)eResolveProxy(oldOutputLayer);
			if (outputLayer != oldOutputLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.REPORT__OUTPUT_LAYER, oldOutputLayer, outputLayer));
			}
		}
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass basicGetOutputLayer() {
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOutputLayer(ELClass newOutputLayer) {
		ELClass oldOutputLayer = outputLayer;
		outputLayer = newOutputLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.REPORT__OUTPUT_LAYER, oldOutputLayer, outputLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReportRow> getRows() {
		if (rows == null) {
			rows = new EObjectContainmentEList<ReportRow>(ReportRow.class, this, Ecore4regPackage.REPORT__ROWS);
		}
		return rows;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReportColumn> getColumns() {
		if (columns == null) {
			columns = new EObjectContainmentEList<ReportColumn>(ReportColumn.class, this, Ecore4regPackage.REPORT__COLUMNS);
		}
		return columns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReportCell> getReportCells() {
		if (reportCells == null) {
			reportCells = new EObjectContainmentEList<ReportCell>(ReportCell.class, this, Ecore4regPackage.REPORT__REPORT_CELLS);
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
			case Ecore4regPackage.REPORT__ROWS:
				return ((InternalEList<?>)getRows()).basicRemove(otherEnd, msgs);
			case Ecore4regPackage.REPORT__COLUMNS:
				return ((InternalEList<?>)getColumns()).basicRemove(otherEnd, msgs);
			case Ecore4regPackage.REPORT__REPORT_CELLS:
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
			case Ecore4regPackage.REPORT__OUTPUT_LAYER:
				if (resolve) return getOutputLayer();
				return basicGetOutputLayer();
			case Ecore4regPackage.REPORT__ROWS:
				return getRows();
			case Ecore4regPackage.REPORT__COLUMNS:
				return getColumns();
			case Ecore4regPackage.REPORT__REPORT_CELLS:
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
			case Ecore4regPackage.REPORT__OUTPUT_LAYER:
				setOutputLayer((ELClass)newValue);
				return;
			case Ecore4regPackage.REPORT__ROWS:
				getRows().clear();
				getRows().addAll((Collection<? extends ReportRow>)newValue);
				return;
			case Ecore4regPackage.REPORT__COLUMNS:
				getColumns().clear();
				getColumns().addAll((Collection<? extends ReportColumn>)newValue);
				return;
			case Ecore4regPackage.REPORT__REPORT_CELLS:
				getReportCells().clear();
				getReportCells().addAll((Collection<? extends ReportCell>)newValue);
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
			case Ecore4regPackage.REPORT__OUTPUT_LAYER:
				setOutputLayer((ELClass)null);
				return;
			case Ecore4regPackage.REPORT__ROWS:
				getRows().clear();
				return;
			case Ecore4regPackage.REPORT__COLUMNS:
				getColumns().clear();
				return;
			case Ecore4regPackage.REPORT__REPORT_CELLS:
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
			case Ecore4regPackage.REPORT__OUTPUT_LAYER:
				return outputLayer != null;
			case Ecore4regPackage.REPORT__ROWS:
				return rows != null && !rows.isEmpty();
			case Ecore4regPackage.REPORT__COLUMNS:
				return columns != null && !columns.isEmpty();
			case Ecore4regPackage.REPORT__REPORT_CELLS:
				return reportCells != null && !reportCells.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ReportImpl
