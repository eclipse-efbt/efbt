/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage
 * @generated
 */
public interface ReportsFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	ReportsFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Axis Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Axis Module</em>'.
	 * @generated
	 */
	AxisModule createAxisModule();

	/**
	 * Returns a new object of class '<em>Axis Ordinate Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Axis Ordinate Module</em>'.
	 * @generated
	 */
	AxisOrdinateModule createAxisOrdinateModule();

	/**
	 * Returns a new object of class '<em>Cell Position Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell Position Module</em>'.
	 * @generated
	 */
	CellPositionModule createCellPositionModule();

	/**
	 * Returns a new object of class '<em>Ordinate Item Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Ordinate Item Module</em>'.
	 * @generated
	 */
	OrdinateItemModule createOrdinateItemModule();

	/**
	 * Returns a new object of class '<em>Populated Report</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Populated Report</em>'.
	 * @generated
	 */
	PopulatedReport createPopulatedReport();

	/**
	 * Returns a new object of class '<em>Report Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell</em>'.
	 * @generated
	 */
	ReportCell createReportCell();

	/**
	 * Returns a new object of class '<em>Report Cell Value</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell Value</em>'.
	 * @generated
	 */
	ReportCellValue createReportCellValue();

	/**
	 * Returns a new object of class '<em>Report Table Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Table Module</em>'.
	 * @generated
	 */
	ReportTableModule createReportTableModule();

	/**
	 * Returns a new object of class '<em>Table Cell Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Table Cell Module</em>'.
	 * @generated
	 */
	TableCellModule createTableCellModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	ReportsPackage getReportsPackage();

} //ReportsFactory
