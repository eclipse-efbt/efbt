/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Cell View Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of ReportCellViews 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule#getReportCellViews <em>Report Cell Views</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage#getReportCellViewModule()
 * @model extendedMetaData="name='ReportCellViewModule' kind='elementOnly'"
 * @generated
 */
public interface ReportCellViewModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Report Cell Views</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellView}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ReportCellViews
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cell Views</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage#getReportCellViewModule_ReportCellViews()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportCellViews'"
	 * @generated
	 */
	EList<ReportCellView> getReportCellViews();

} // ReportCellViewModule
