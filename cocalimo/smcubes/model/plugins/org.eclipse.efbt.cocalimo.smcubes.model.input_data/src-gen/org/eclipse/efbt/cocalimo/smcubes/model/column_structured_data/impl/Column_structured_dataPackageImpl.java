/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

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
public class Column_structured_dataPackageImpl extends EPackageImpl implements Column_structured_dataPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnStructuredDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnStructuredDataModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowDataEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Column_structured_dataPackageImpl()
	{
		super(eNS_URI, Column_structured_dataFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Column_structured_dataPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Column_structured_dataPackage init()
	{
		if (isInited) return (Column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Column_structured_dataPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredColumn_structured_dataPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Column_structured_dataPackageImpl theColumn_structured_dataPackage = registeredColumn_structured_dataPackage instanceof Column_structured_dataPackageImpl ? (Column_structured_dataPackageImpl)registeredColumn_structured_dataPackage : new Column_structured_dataPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Base_column_structured_dataPackage.eNS_URI);
		Base_column_structured_dataPackageImpl theBase_column_structured_dataPackage = (Base_column_structured_dataPackageImpl)(registeredPackage instanceof Base_column_structured_dataPackageImpl ? registeredPackage : Base_column_structured_dataPackage.eINSTANCE);

		// Create package meta-data objects
		theColumn_structured_dataPackage.createPackageContents();
		theBase_column_structured_dataPackage.createPackageContents();

		// Initialize created meta-data
		theColumn_structured_dataPackage.initializePackageContents();
		theBase_column_structured_dataPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theColumn_structured_dataPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Column_structured_dataPackage.eNS_URI, theColumn_structured_dataPackage);
		return theColumn_structured_dataPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCell()
	{
		return cellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCell_CellID()
	{
		return (EAttribute)cellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCell_Column()
	{
		return (EReference)cellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnStructuredData()
	{
		return columnStructuredDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getColumnStructuredData_Name()
	{
		return (EAttribute)columnStructuredDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnStructuredDataModule()
	{
		return columnStructuredDataModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnStructuredDataModule_CubeData()
	{
		return (EReference)columnStructuredDataModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRowData()
	{
		return rowDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getRowData_RowID()
	{
		return (EAttribute)rowDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Column_structured_dataFactory getColumn_structured_dataFactory()
	{
		return (Column_structured_dataFactory)getEFactoryInstance();
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
		cellEClass = createEClass(CELL);
		createEAttribute(cellEClass, CELL__CELL_ID);
		createEReference(cellEClass, CELL__COLUMN);

		columnStructuredDataEClass = createEClass(COLUMN_STRUCTURED_DATA);
		createEAttribute(columnStructuredDataEClass, COLUMN_STRUCTURED_DATA__NAME);

		columnStructuredDataModuleEClass = createEClass(COLUMN_STRUCTURED_DATA_MODULE);
		createEReference(columnStructuredDataModuleEClass, COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA);

		rowDataEClass = createEClass(ROW_DATA);
		createEAttribute(rowDataEClass, ROW_DATA__ROW_ID);
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
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		columnStructuredDataModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(cellEClass, Cell.class, "Cell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCell_CellID(), ecorePackage.getEString(), "cellID", null, 0, 1, Cell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCell_Column(), theCorePackage.getVARIABLE(), null, "column", null, 0, 1, Cell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnStructuredDataEClass, ColumnStructuredData.class, "ColumnStructuredData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getColumnStructuredData_Name(), ecorePackage.getEString(), "name", null, 0, 1, ColumnStructuredData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnStructuredDataModuleEClass, ColumnStructuredDataModule.class, "ColumnStructuredDataModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumnStructuredDataModule_CubeData(), this.getColumnStructuredData(), null, "cubeData", null, 0, -1, ColumnStructuredDataModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowDataEClass, RowData.class, "RowData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRowData_RowID(), ecorePackage.getEString(), "rowID", null, 0, 1, RowData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createExtendedMetaDataAnnotations()
	{
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (getCell_CellID(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cellID"
		   });
		addAnnotation
		  (getCell_Column(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "column"
		   });
		addAnnotation
		  (getColumnStructuredData_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (columnStructuredDataModuleEClass,
		   source,
		   new String[] {
			   "name", "ColumnStructuredDataModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getColumnStructuredDataModule_CubeData(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeData"
		   });
		addAnnotation
		  (getRowData_RowID(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "rowID"
		   });
	}

} //Column_structured_dataPackageImpl
