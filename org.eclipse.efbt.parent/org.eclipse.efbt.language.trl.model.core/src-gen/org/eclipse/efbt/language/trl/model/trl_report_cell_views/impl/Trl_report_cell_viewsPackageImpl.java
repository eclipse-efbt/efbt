/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.data_structures.smcubes.model.core.CorePackage;

import org.eclipse.efbt.data_structures.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.data_structures.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.data_structures.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.functional_design.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.functional_design.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.Domain_model_mappingPackageImpl;

import org.eclipse.efbt.language.trl.model.transformation.TransformationPackage;

import org.eclipse.efbt.language.trl.model.transformation.impl.TransformationPackageImpl;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsFactory;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.lineage.common.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

import org.eclipse.efbt.requirements.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Trl_report_cell_viewsPackageImpl extends EPackageImpl implements Trl_report_cell_viewsPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellViewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellViewModuleEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Trl_report_cell_viewsPackageImpl()
	{
		super(eNS_URI, Trl_report_cell_viewsFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Trl_report_cell_viewsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Trl_report_cell_viewsPackage init()
	{
		if (isInited) return (Trl_report_cell_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(Trl_report_cell_viewsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTrl_report_cell_viewsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Trl_report_cell_viewsPackageImpl theTrl_report_cell_viewsPackage = registeredTrl_report_cell_viewsPackage instanceof Trl_report_cell_viewsPackageImpl ? (Trl_report_cell_viewsPackageImpl)registeredTrl_report_cell_viewsPackage : new Trl_report_cell_viewsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		LineagefunctionsPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = (Domain_model_mappingPackageImpl)(registeredPackage instanceof Domain_model_mappingPackageImpl ? registeredPackage : Domain_model_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		TransformationPackageImpl theTransformationPackage = (TransformationPackageImpl)(registeredPackage instanceof TransformationPackageImpl ? registeredPackage : TransformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Trl_sql_viewsPackage.eNS_URI);
		Trl_sql_viewsPackageImpl theTrl_sql_viewsPackage = (Trl_sql_viewsPackageImpl)(registeredPackage instanceof Trl_sql_viewsPackageImpl ? registeredPackage : Trl_sql_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		theTrl_report_cell_viewsPackage.createPackageContents();
		theDomain_model_mappingPackage.createPackageContents();
		theTransformationPackage.createPackageContents();
		theTrl_sql_viewsPackage.createPackageContents();

		// Initialize created meta-data
		theTrl_report_cell_viewsPackage.initializePackageContents();
		theDomain_model_mappingPackage.initializePackageContents();
		theTransformationPackage.initializePackageContents();
		theTrl_sql_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTrl_report_cell_viewsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Trl_report_cell_viewsPackage.eNS_URI, theTrl_report_cell_viewsPackage);
		return theTrl_report_cell_viewsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellView()
	{
		return reportCellViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellView_ReportCell()
	{
		return (EReference)reportCellViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellViewModule()
	{
		return reportCellViewModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellViewModule_ReportCellViews()
	{
		return (EReference)reportCellViewModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Trl_report_cell_viewsFactory getTrl_report_cell_viewsFactory()
	{
		return (Trl_report_cell_viewsFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents()
	{
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		reportCellViewEClass = createEClass(REPORT_CELL_VIEW);
		createEReference(reportCellViewEClass, REPORT_CELL_VIEW__REPORT_CELL);

		reportCellViewModuleEClass = createEClass(REPORT_CELL_VIEW_MODULE);
		createEReference(reportCellViewModuleEClass, REPORT_CELL_VIEW_MODULE__REPORT_CELL_VIEWS);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents()
	{
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Trl_sql_viewsPackage theTrl_sql_viewsPackage = (Trl_sql_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(Trl_sql_viewsPackage.eNS_URI);
		ReportsPackage theReportsPackage = (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		reportCellViewEClass.getESuperTypes().add(theTrl_sql_viewsPackage.getSQLView());
		reportCellViewModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(reportCellViewEClass, ReportCellView.class, "ReportCellView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCellView_ReportCell(), theReportsPackage.getReportCell(), null, "reportCell", null, 0, 1, ReportCellView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellViewModuleEClass, ReportCellViewModule.class, "ReportCellViewModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCellViewModule_ReportCellViews(), this.getReportCellView(), null, "reportCellViews", null, 0, -1, ReportCellViewModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations()
	{
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[]
		   {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations()
	{
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (reportCellViewEClass,
		   source,
		   new String[]
		   {
			   "name", "ReportCellView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportCellView_ReportCell(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "reportCell"
		   });
		addAnnotation
		  (reportCellViewModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "ReportCellViewModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportCellViewModule_ReportCellViews(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "reportCellViews"
		   });
	}

} //Trl_report_cell_viewsPackageImpl
