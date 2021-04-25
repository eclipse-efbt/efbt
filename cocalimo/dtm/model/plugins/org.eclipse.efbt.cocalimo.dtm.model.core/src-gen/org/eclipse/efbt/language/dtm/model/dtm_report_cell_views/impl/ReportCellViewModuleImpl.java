/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Cell View Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.ReportCellViewModuleImpl#getReportCellViews <em>Report Cell Views</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellViewModuleImpl extends ModuleImpl implements ReportCellViewModule {
	/**
	 * The cached value of the '{@link #getReportCellViews() <em>Report Cell Views</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCellViews()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellView> reportCellViews;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellViewModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_report_cell_viewsPackage.Literals.REPORT_CELL_VIEW_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReportCellView> getReportCellViews() {
		if (reportCellViews == null) {
			reportCellViews = new EObjectContainmentEList<ReportCellView>(ReportCellView.class, this, dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS);
		}
		return reportCellViews;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS:
				return ((InternalEList<?>)getReportCellViews()).basicRemove(otherEnd, msgs);
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS:
				return getReportCellViews();
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS:
				getReportCellViews().clear();
				getReportCellViews().addAll((Collection<? extends ReportCellView>)newValue);
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS:
				getReportCellViews().clear();
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS:
				return reportCellViews != null && !reportCellViews.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ReportCellViewModuleImpl
