/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl.Efbt_mappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.AxisModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.AxisOrdinateModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.CellPositionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.OrdinateItemModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.PopulatedReport;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCell;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportCellValue;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportTableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.TableCellModule;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl;

import org.eclipse.emf.ecore.EAttribute;
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
public class ReportsPackageImpl extends EPackageImpl implements ReportsPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axisModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axisOrdinateModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellPositionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass ordinateItemModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass populatedReportEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellValueEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportTableModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tableCellModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private ReportsPackageImpl() {
		super(eNS_URI, ReportsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link ReportsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static ReportsPackage init() {
		if (isInited) return (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredReportsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		ReportsPackageImpl theReportsPackage = registeredReportsPackage instanceof ReportsPackageImpl ? (ReportsPackageImpl)registeredReportsPackage : new ReportsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_data_definitionPackage.eNS_URI);
		Efbt_data_definitionPackageImpl theEfbt_data_definitionPackage = (Efbt_data_definitionPackageImpl)(registeredPackage instanceof Efbt_data_definitionPackageImpl ? registeredPackage : Efbt_data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_mappingPackage.eNS_URI);
		Efbt_mappingPackageImpl theEfbt_mappingPackage = (Efbt_mappingPackageImpl)(registeredPackage instanceof Efbt_mappingPackageImpl ? registeredPackage : Efbt_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Smcubes_modelPackage.eNS_URI);
		Smcubes_modelPackageImpl theSmcubes_modelPackage = (Smcubes_modelPackageImpl)(registeredPackage instanceof Smcubes_modelPackageImpl ? registeredPackage : Smcubes_modelPackage.eINSTANCE);

		// Create package meta-data objects
		theReportsPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theEfbt_data_definitionPackage.createPackageContents();
		theEfbt_mappingPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theSmcubes_modelPackage.createPackageContents();

		// Initialize created meta-data
		theReportsPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theEfbt_data_definitionPackage.initializePackageContents();
		theEfbt_mappingPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theSmcubes_modelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theReportsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(ReportsPackage.eNS_URI, theReportsPackage);
		return theReportsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAxisModule() {
		return axisModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAxisModule_Axes() {
		return (EReference)axisModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAxisOrdinateModule() {
		return axisOrdinateModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAxisOrdinateModule_Axis_ordinates() {
		return (EReference)axisOrdinateModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCellPositionModule() {
		return cellPositionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCellPositionModule_ReportTables() {
		return (EReference)cellPositionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getOrdinateItemModule() {
		return ordinateItemModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getOrdinateItemModule_OrdinateItems() {
		return (EReference)ordinateItemModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPopulatedReport() {
		return populatedReportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getPopulatedReport_ReportCellValues() {
		return (EReference)populatedReportEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPopulatedReport_Name() {
		return (EAttribute)populatedReportEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCell() {
		return reportCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Combination() {
		return (EReference)reportCellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportCell_Name() {
		return (EAttribute)reportCellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellValue() {
		return reportCellValueEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellValue_ReportCell() {
		return (EReference)reportCellValueEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportCellValue_Value() {
		return (EAttribute)reportCellValueEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportTableModule() {
		return reportTableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportTableModule_ReportTables() {
		return (EReference)reportTableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTableCellModule() {
		return tableCellModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTableCellModule_TableCells() {
		return (EReference)tableCellModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportsFactory getReportsFactory() {
		return (ReportsFactory)getEFactoryInstance();
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
		axisModuleEClass = createEClass(AXIS_MODULE);
		createEReference(axisModuleEClass, AXIS_MODULE__AXES);

		axisOrdinateModuleEClass = createEClass(AXIS_ORDINATE_MODULE);
		createEReference(axisOrdinateModuleEClass, AXIS_ORDINATE_MODULE__AXIS_ORDINATES);

		cellPositionModuleEClass = createEClass(CELL_POSITION_MODULE);
		createEReference(cellPositionModuleEClass, CELL_POSITION_MODULE__REPORT_TABLES);

		ordinateItemModuleEClass = createEClass(ORDINATE_ITEM_MODULE);
		createEReference(ordinateItemModuleEClass, ORDINATE_ITEM_MODULE__ORDINATE_ITEMS);

		populatedReportEClass = createEClass(POPULATED_REPORT);
		createEReference(populatedReportEClass, POPULATED_REPORT__REPORT_CELL_VALUES);
		createEAttribute(populatedReportEClass, POPULATED_REPORT__NAME);

		reportCellEClass = createEClass(REPORT_CELL);
		createEReference(reportCellEClass, REPORT_CELL__COMBINATION);
		createEAttribute(reportCellEClass, REPORT_CELL__NAME);

		reportCellValueEClass = createEClass(REPORT_CELL_VALUE);
		createEReference(reportCellValueEClass, REPORT_CELL_VALUE__REPORT_CELL);
		createEAttribute(reportCellValueEClass, REPORT_CELL_VALUE__VALUE);

		reportTableModuleEClass = createEClass(REPORT_TABLE_MODULE);
		createEReference(reportTableModuleEClass, REPORT_TABLE_MODULE__REPORT_TABLES);

		tableCellModuleEClass = createEClass(TABLE_CELL_MODULE);
		createEReference(tableCellModuleEClass, TABLE_CELL_MODULE__TABLE_CELLS);
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
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		RenderingPackage theRenderingPackage = (RenderingPackage)EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		axisModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		axisOrdinateModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cellPositionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		ordinateItemModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		reportTableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		tableCellModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(axisModuleEClass, AxisModule.class, "AxisModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAxisModule_Axes(), theRenderingPackage.getAXIS(), null, "axes", null, 0, -1, AxisModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(axisOrdinateModuleEClass, AxisOrdinateModule.class, "AxisOrdinateModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAxisOrdinateModule_Axis_ordinates(), theRenderingPackage.getAXIS_ORDINATE(), null, "axis_ordinates", null, 0, -1, AxisOrdinateModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cellPositionModuleEClass, CellPositionModule.class, "CellPositionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCellPositionModule_ReportTables(), theRenderingPackage.getCELL_POSITION(), null, "reportTables", null, 0, -1, CellPositionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(ordinateItemModuleEClass, OrdinateItemModule.class, "OrdinateItemModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getOrdinateItemModule_OrdinateItems(), theRenderingPackage.getORDINATE_ITEM(), null, "ordinateItems", null, 0, -1, OrdinateItemModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(populatedReportEClass, PopulatedReport.class, "PopulatedReport", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getPopulatedReport_ReportCellValues(), this.getReportCellValue(), null, "reportCellValues", null, 0, 1, PopulatedReport.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPopulatedReport_Name(), ecorePackage.getEString(), "name", null, 0, 1, PopulatedReport.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellEClass, ReportCell.class, "ReportCell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCell_Combination(), theData_definitionPackage.getCOMBINATION(), null, "combination", null, 0, 1, ReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReportCell_Name(), ecorePackage.getEString(), "name", null, 0, 1, ReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellValueEClass, ReportCellValue.class, "ReportCellValue", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCellValue_ReportCell(), this.getReportCell(), null, "reportCell", null, 0, 1, ReportCellValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReportCellValue_Value(), ecorePackage.getEString(), "value", null, 0, 1, ReportCellValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportTableModuleEClass, ReportTableModule.class, "ReportTableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportTableModule_ReportTables(), theRenderingPackage.getTABLE(), null, "reportTables", null, 0, -1, ReportTableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tableCellModuleEClass, TableCellModule.class, "TableCellModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTableCellModule_TableCells(), theRenderingPackage.getTABLE_CELL(), null, "tableCells", null, 0, -1, TableCellModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (axisModuleEClass,
		   source,
		   new String[] {
			   "name", "AxisModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAxisModule_Axes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "axes"
		   });
		addAnnotation
		  (axisOrdinateModuleEClass,
		   source,
		   new String[] {
			   "name", "AxisOrdinateModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAxisOrdinateModule_Axis_ordinates(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "axis_ordinates"
		   });
		addAnnotation
		  (cellPositionModuleEClass,
		   source,
		   new String[] {
			   "name", "CellPositionModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCellPositionModule_ReportTables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportTables"
		   });
		addAnnotation
		  (ordinateItemModuleEClass,
		   source,
		   new String[] {
			   "name", "OrdinateItemModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getOrdinateItemModule_OrdinateItems(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "ordinateItems"
		   });
		addAnnotation
		  (populatedReportEClass,
		   source,
		   new String[] {
			   "name", "PopulatedReport",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getPopulatedReport_ReportCellValues(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportCellValues"
		   });
		addAnnotation
		  (getPopulatedReport_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (reportCellEClass,
		   source,
		   new String[] {
			   "name", "ReportCell",
			   "kind", "empty"
		   });
		addAnnotation
		  (getReportCell_Combination(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "combination"
		   });
		addAnnotation
		  (reportCellValueEClass,
		   source,
		   new String[] {
			   "name", "ReportCellValue",
			   "kind", "empty"
		   });
		addAnnotation
		  (getReportCellValue_ReportCell(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "reportCell"
		   });
		addAnnotation
		  (reportTableModuleEClass,
		   source,
		   new String[] {
			   "name", "ReportTableModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportTableModule_ReportTables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportTables"
		   });
		addAnnotation
		  (tableCellModuleEClass,
		   source,
		   new String[] {
			   "name", "TableCellModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTableCellModule_TableCells(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "tableCells"
		   });
	}

} //ReportsPackageImpl
