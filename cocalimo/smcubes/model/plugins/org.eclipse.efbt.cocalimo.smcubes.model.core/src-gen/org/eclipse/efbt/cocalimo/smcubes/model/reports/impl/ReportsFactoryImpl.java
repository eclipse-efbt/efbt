/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class ReportsFactoryImpl extends EFactoryImpl implements ReportsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static ReportsFactory init() {
		try {
			ReportsFactory theReportsFactory = (ReportsFactory)EPackage.Registry.INSTANCE.getEFactory(ReportsPackage.eNS_URI);
			if (theReportsFactory != null) {
				return theReportsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new ReportsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportsFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case ReportsPackage.AXIS_MODULE: return createAxisModule();
			case ReportsPackage.AXIS_ORDINATE_MODULE: return createAxisOrdinateModule();
			case ReportsPackage.CELL_POSITION_MODULE: return createCellPositionModule();
			case ReportsPackage.ORDINATE_ITEM_MODULE: return createOrdinateItemModule();
			case ReportsPackage.POPULATED_REPORT: return createPopulatedReport();
			case ReportsPackage.REPORT_CELL: return createReportCell();
			case ReportsPackage.REPORT_CELL_VALUE: return createReportCellValue();
			case ReportsPackage.REPORT_TABLE_MODULE: return createReportTableModule();
			case ReportsPackage.TABLE_CELL_MODULE: return createTableCellModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AxisModule createAxisModule() {
		AxisModuleImpl axisModule = new AxisModuleImpl();
		return axisModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AxisOrdinateModule createAxisOrdinateModule() {
		AxisOrdinateModuleImpl axisOrdinateModule = new AxisOrdinateModuleImpl();
		return axisOrdinateModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CellPositionModule createCellPositionModule() {
		CellPositionModuleImpl cellPositionModule = new CellPositionModuleImpl();
		return cellPositionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public OrdinateItemModule createOrdinateItemModule() {
		OrdinateItemModuleImpl ordinateItemModule = new OrdinateItemModuleImpl();
		return ordinateItemModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PopulatedReport createPopulatedReport() {
		PopulatedReportImpl populatedReport = new PopulatedReportImpl();
		return populatedReport;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell createReportCell() {
		ReportCellImpl reportCell = new ReportCellImpl();
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellValue createReportCellValue() {
		ReportCellValueImpl reportCellValue = new ReportCellValueImpl();
		return reportCellValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportTableModule createReportTableModule() {
		ReportTableModuleImpl reportTableModule = new ReportTableModuleImpl();
		return reportTableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TableCellModule createTableCellModule() {
		TableCellModuleImpl tableCellModule = new TableCellModuleImpl();
		return tableCellModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportsPackage getReportsPackage() {
		return (ReportsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static ReportsPackage getPackage() {
		return ReportsPackage.eINSTANCE;
	}

} //ReportsFactoryImpl
