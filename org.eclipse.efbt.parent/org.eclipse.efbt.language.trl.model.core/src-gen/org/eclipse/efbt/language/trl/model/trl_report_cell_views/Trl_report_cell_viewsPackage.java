/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * <!-- begin-model-doc -->
 *  This package describes TRL preport cell views
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Trl_report_cell_viewsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "trl_report_cell_views";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/trl_report_cell_views";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "trl_report_cell_views";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Trl_report_cell_viewsPackage eINSTANCE = org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewImpl <em>Report Cell View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl#getReportCellView()
	 * @generated
	 */
	int REPORT_CELL_VIEW = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW__CUBE = Trl_sql_viewsPackage.SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW__NAME = Trl_sql_viewsPackage.SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW__VTL_CODE = Trl_sql_viewsPackage.SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Report Cell</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW__REPORT_CELL = Trl_sql_viewsPackage.SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Report Cell View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_FEATURE_COUNT = Trl_sql_viewsPackage.SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Report Cell View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_OPERATION_COUNT = Trl_sql_viewsPackage.SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewModuleImpl <em>Report Cell View Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewModuleImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl#getReportCellViewModule()
	 * @generated
	 */
	int REPORT_CELL_VIEW_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Report Cell Views</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Report Cell View Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Report Cell View Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VIEW_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView <em>Report Cell View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView
	 * @generated
	 */
	EClass getReportCellView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView#getReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Report Cell</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView#getReportCell()
	 * @see #getReportCellView()
	 * @generated
	 */
	EReference getReportCellView_ReportCell();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule <em>Report Cell View Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell View Module</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule
	 * @generated
	 */
	EClass getReportCellViewModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule#getReportCellViews <em>Report Cell Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cell Views</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule#getReportCellViews()
	 * @see #getReportCellViewModule()
	 * @generated
	 */
	EReference getReportCellViewModule_ReportCellViews();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Trl_report_cell_viewsFactory getTrl_report_cell_viewsFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewImpl <em>Report Cell View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl#getReportCellView()
		 * @generated
		 */
		EClass REPORT_CELL_VIEW = eINSTANCE.getReportCellView();

		/**
		 * The meta object literal for the '<em><b>Report Cell</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_CELL_VIEW__REPORT_CELL = eINSTANCE.getReportCellView_ReportCell();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewModuleImpl <em>Report Cell View Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.ReportCellViewModuleImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl#getReportCellViewModule()
		 * @generated
		 */
		EClass REPORT_CELL_VIEW_MODULE = eINSTANCE.getReportCellViewModule();

		/**
		 * The meta object literal for the '<em><b>Report Cell Views</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS = eINSTANCE.getReportCellViewModule_ReportCellViews();

	}

} //Trl_report_cell_viewsPackage
