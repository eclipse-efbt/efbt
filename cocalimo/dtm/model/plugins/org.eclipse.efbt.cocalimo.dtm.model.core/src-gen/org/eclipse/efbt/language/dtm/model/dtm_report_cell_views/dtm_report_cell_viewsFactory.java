/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage
 * @generated
 */
public interface dtm_report_cell_viewsFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	dtm_report_cell_viewsFactory eINSTANCE = org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl.dtm_report_cell_viewsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Report Cell View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell View</em>'.
	 * @generated
	 */
	ReportCellView createReportCellView();

	/**
	 * Returns a new object of class '<em>Report Cell View Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell View Module</em>'.
	 * @generated
	 */
	ReportCellViewModule createReportCellViewModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	dtm_report_cell_viewsPackage getdtm_report_cell_viewsPackage();

} //dtm_report_cell_viewsFactory
