/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views;

import org.eclipse.efbt.data_structures.smcubes.model.reports.ReportCell;

import org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Cell View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A ReportCellView is an SQL View which has the explicit purpose of calculating one report cell.
 *       ReportCells are represented as a TargetCube with one row. This cube has multiple Dimensions columns and one Measure Column.
 *       The Dimension fields for table are set to be specific members of that dimension. 
 *       Similar concepts exist in the taxonomy of XBRL reporting language and DataPointModelling.
 *       For example a ReportCellView which has a sourceCube of trade, associated measure 'amount' and associated dimensions Currency and Country,
 *       where it has been specified that Currency will equal USD and Country will equal US, will lead to the amount measure
 *       being populated with the sum of the amount field in the Trade table, for all rows in the Trade Table  where Currency=USD AND Country = US.
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView#getReportCell <em>Report Cell</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage#getReportCellView()
 * @model extendedMetaData="name='ReportCellView' kind='elementOnly'"
 * @generated
 */
public interface ReportCellView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Report Cell</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The report cell
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cell</em>' containment reference.
	 * @see #setReportCell(ReportCell)
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage#getReportCellView_ReportCell()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportCell'"
	 * @generated
	 */
	ReportCell getReportCell();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView#getReportCell <em>Report Cell</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Report Cell</em>' containment reference.
	 * @see #getReportCell()
	 * @generated
	 */
	void setReportCell(ReportCell value);

} // ReportCellView
