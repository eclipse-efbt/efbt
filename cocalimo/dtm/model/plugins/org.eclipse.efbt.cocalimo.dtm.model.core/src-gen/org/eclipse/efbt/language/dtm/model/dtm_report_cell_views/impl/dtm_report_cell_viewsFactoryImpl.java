/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.*;

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
public class dtm_report_cell_viewsFactoryImpl extends EFactoryImpl implements dtm_report_cell_viewsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static dtm_report_cell_viewsFactory init() {
		try {
			dtm_report_cell_viewsFactory thedtm_report_cell_viewsFactory = (dtm_report_cell_viewsFactory)EPackage.Registry.INSTANCE.getEFactory(dtm_report_cell_viewsPackage.eNS_URI);
			if (thedtm_report_cell_viewsFactory != null) {
				return thedtm_report_cell_viewsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new dtm_report_cell_viewsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public dtm_report_cell_viewsFactoryImpl() {
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
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW: return createReportCellView();
			case dtm_report_cell_viewsPackage.REPORT_CELL_VIEW_MODULE: return createReportCellViewModule();
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
	public ReportCellView createReportCellView() {
		ReportCellViewImpl reportCellView = new ReportCellViewImpl();
		return reportCellView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellViewModule createReportCellViewModule() {
		ReportCellViewModuleImpl reportCellViewModule = new ReportCellViewModuleImpl();
		return reportCellViewModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public dtm_report_cell_viewsPackage getdtm_report_cell_viewsPackage() {
		return (dtm_report_cell_viewsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static dtm_report_cell_viewsPackage getPackage() {
		return dtm_report_cell_viewsPackage.eINSTANCE;
	}

} //dtm_report_cell_viewsFactoryImpl
