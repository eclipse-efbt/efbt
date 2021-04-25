/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl.Domain_model_mappingPackageImpl;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsFactory;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;

import org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl;

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
public class dtm_report_cell_viewsPackageImpl extends EPackageImpl implements dtm_report_cell_viewsPackage {
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
	 * @see org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private dtm_report_cell_viewsPackageImpl() {
		super(eNS_URI, dtm_report_cell_viewsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link dtm_report_cell_viewsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static dtm_report_cell_viewsPackage init() {
		if (isInited) return (dtm_report_cell_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(dtm_report_cell_viewsPackage.eNS_URI);

		// Obtain or create and register package
		Object registereddtm_report_cell_viewsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		dtm_report_cell_viewsPackageImpl thedtm_report_cell_viewsPackage = registereddtm_report_cell_viewsPackage instanceof dtm_report_cell_viewsPackageImpl ? (dtm_report_cell_viewsPackageImpl)registereddtm_report_cell_viewsPackage : new dtm_report_cell_viewsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		ProgramPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		Efbt_data_definitionPackage.eINSTANCE.eClass();
		Efbt_mappingPackage.eINSTANCE.eClass();
		Efbt_vtl_transformationPackage.eINSTANCE.eClass();
		MappingPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();
		Smcubes_modelPackage.eINSTANCE.eClass();
		Vtl_transformationPackage.eINSTANCE.eClass();
		Test_definitionPackage.eINSTANCE.eClass();
		Test_input_dataPackage.eINSTANCE.eClass();
		TestPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = (Domain_model_mappingPackageImpl)(registeredPackage instanceof Domain_model_mappingPackageImpl ? registeredPackage : Domain_model_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		TransformationPackageImpl theTransformationPackage = (TransformationPackageImpl)(registeredPackage instanceof TransformationPackageImpl ? registeredPackage : TransformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(dtm_viewsPackage.eNS_URI);
		dtm_viewsPackageImpl thedtm_viewsPackage = (dtm_viewsPackageImpl)(registeredPackage instanceof dtm_viewsPackageImpl ? registeredPackage : dtm_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		thedtm_report_cell_viewsPackage.createPackageContents();
		theDomain_model_mappingPackage.createPackageContents();
		theTransformationPackage.createPackageContents();
		thedtm_viewsPackage.createPackageContents();

		// Initialize created meta-data
		thedtm_report_cell_viewsPackage.initializePackageContents();
		theDomain_model_mappingPackage.initializePackageContents();
		theTransformationPackage.initializePackageContents();
		thedtm_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		thedtm_report_cell_viewsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(dtm_report_cell_viewsPackage.eNS_URI, thedtm_report_cell_viewsPackage);
		return thedtm_report_cell_viewsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellView() {
		return reportCellViewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellView_ReportCell() {
		return (EReference)reportCellViewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellView_StructColumnHoldingDimension() {
		return (EReference)reportCellViewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellView_StructColumnHoldingMeasure() {
		return (EReference)reportCellViewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellViewModule() {
		return reportCellViewModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellViewModule_ReportCellViews() {
		return (EReference)reportCellViewModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public dtm_report_cell_viewsFactory getdtm_report_cell_viewsFactory() {
		return (dtm_report_cell_viewsFactory)getEFactoryInstance();
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
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		reportCellViewEClass = createEClass(REPORT_CELL_VIEW);
		createEReference(reportCellViewEClass, REPORT_CELL_VIEW__REPORT_CELL);
		createEReference(reportCellViewEClass, REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_DIMENSION);
		createEReference(reportCellViewEClass, REPORT_CELL_VIEW__STRUCT_COLUMN_HOLDING_MEASURE);

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
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		dtm_viewsPackage thedtm_viewsPackage = (dtm_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(dtm_viewsPackage.eNS_URI);
		ReportsPackage theReportsPackage = (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);
		Efbt_advanced_data_definitionPackage theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		reportCellViewEClass.getESuperTypes().add(thedtm_viewsPackage.getDTMView());
		reportCellViewModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(reportCellViewEClass, ReportCellView.class, "ReportCellView", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCellView_ReportCell(), theReportsPackage.getReportCell(), null, "reportCell", null, 0, 1, ReportCellView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getReportCellView_StructColumnHoldingDimension(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "structColumnHoldingDimension", null, 0, 1, ReportCellView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getReportCellView_StructColumnHoldingMeasure(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "structColumnHoldingMeasure", null, 0, 1, ReportCellView.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations() {
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (reportCellViewEClass,
		   source,
		   new String[] {
			   "name", "ReportCellView",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportCellView_ReportCell(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportCell"
		   });
		addAnnotation
		  (reportCellViewModuleEClass,
		   source,
		   new String[] {
			   "name", "ReportCellViewModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportCellViewModule_ReportCellViews(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportCellViews"
		   });
	}

} //dtm_report_cell_viewsPackageImpl
