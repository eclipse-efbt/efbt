/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.impl;

import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCell;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataFactory;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputDataModule;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl;

import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionPackage;

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
public class Base_column_structured_dataPackageImpl extends EPackageImpl implements Base_column_structured_dataPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseCellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseCellWithEnumeratedValueEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseCellWithValueEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseColumnStructuredDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseRowDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass inputDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass inputDataModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Base_column_structured_dataPackageImpl()
	{
		super(eNS_URI, Base_column_structured_dataFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Base_column_structured_dataPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Base_column_structured_dataPackage init()
	{
		if (isInited) return (Base_column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Base_column_structured_dataPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredBase_column_structured_dataPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Base_column_structured_dataPackageImpl theBase_column_structured_dataPackage = registeredBase_column_structured_dataPackage instanceof Base_column_structured_dataPackageImpl ? (Base_column_structured_dataPackageImpl)registeredBase_column_structured_dataPackage : new Base_column_structured_dataPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Column_structured_dataPackage.eNS_URI);
		Column_structured_dataPackageImpl theColumn_structured_dataPackage = (Column_structured_dataPackageImpl)(registeredPackage instanceof Column_structured_dataPackageImpl ? registeredPackage : Column_structured_dataPackage.eINSTANCE);

		// Create package meta-data objects
		theBase_column_structured_dataPackage.createPackageContents();
		theColumn_structured_dataPackage.createPackageContents();

		// Initialize created meta-data
		theBase_column_structured_dataPackage.initializePackageContents();
		theColumn_structured_dataPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theBase_column_structured_dataPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Base_column_structured_dataPackage.eNS_URI, theBase_column_structured_dataPackage);
		return theBase_column_structured_dataPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseCell()
	{
		return baseCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseCellWithEnumeratedValue()
	{
		return baseCellWithEnumeratedValueEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseCellWithEnumeratedValue_Value()
	{
		return (EReference)baseCellWithEnumeratedValueEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseCellWithValue()
	{
		return baseCellWithValueEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getBaseCellWithValue_Value()
	{
		return (EAttribute)baseCellWithValueEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseColumnStructuredData()
	{
		return baseColumnStructuredDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseColumnStructuredData_Rows()
	{
		return (EReference)baseColumnStructuredDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseColumnStructuredData_Cube()
	{
		return (EReference)baseColumnStructuredDataEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseRowData()
	{
		return baseRowDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseRowData_Cells()
	{
		return (EReference)baseRowDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getInputData()
	{
		return inputDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getInputData_SourceTableData()
	{
		return (EReference)inputDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getInputData_Name()
	{
		return (EAttribute)inputDataEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getInputDataModule()
	{
		return inputDataModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getInputDataModule_InputData()
	{
		return (EReference)inputDataModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Base_column_structured_dataFactory getBase_column_structured_dataFactory()
	{
		return (Base_column_structured_dataFactory)getEFactoryInstance();
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
		baseCellEClass = createEClass(BASE_CELL);

		baseCellWithEnumeratedValueEClass = createEClass(BASE_CELL_WITH_ENUMERATED_VALUE);
		createEReference(baseCellWithEnumeratedValueEClass, BASE_CELL_WITH_ENUMERATED_VALUE__VALUE);

		baseCellWithValueEClass = createEClass(BASE_CELL_WITH_VALUE);
		createEAttribute(baseCellWithValueEClass, BASE_CELL_WITH_VALUE__VALUE);

		baseColumnStructuredDataEClass = createEClass(BASE_COLUMN_STRUCTURED_DATA);
		createEReference(baseColumnStructuredDataEClass, BASE_COLUMN_STRUCTURED_DATA__ROWS);
		createEReference(baseColumnStructuredDataEClass, BASE_COLUMN_STRUCTURED_DATA__CUBE);

		baseRowDataEClass = createEClass(BASE_ROW_DATA);
		createEReference(baseRowDataEClass, BASE_ROW_DATA__CELLS);

		inputDataEClass = createEClass(INPUT_DATA);
		createEReference(inputDataEClass, INPUT_DATA__SOURCE_TABLE_DATA);
		createEAttribute(inputDataEClass, INPUT_DATA__NAME);

		inputDataModuleEClass = createEClass(INPUT_DATA_MODULE);
		createEReference(inputDataModuleEClass, INPUT_DATA_MODULE__INPUT_DATA);
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
		Column_structured_dataPackage theColumn_structured_dataPackage = (Column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Column_structured_dataPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		baseCellEClass.getESuperTypes().add(theColumn_structured_dataPackage.getCell());
		baseCellWithEnumeratedValueEClass.getESuperTypes().add(this.getBaseCell());
		baseCellWithValueEClass.getESuperTypes().add(this.getBaseCell());
		baseColumnStructuredDataEClass.getESuperTypes().add(theColumn_structured_dataPackage.getColumnStructuredData());
		baseRowDataEClass.getESuperTypes().add(theColumn_structured_dataPackage.getRowData());
		inputDataModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(baseCellEClass, BaseCell.class, "BaseCell", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(baseCellWithEnumeratedValueEClass, BaseCellWithEnumeratedValue.class, "BaseCellWithEnumeratedValue", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseCellWithEnumeratedValue_Value(), theCorePackage.getMEMBER(), null, "value", null, 0, 1, BaseCellWithEnumeratedValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseCellWithValueEClass, BaseCellWithValue.class, "BaseCellWithValue", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBaseCellWithValue_Value(), ecorePackage.getEString(), "value", null, 0, 1, BaseCellWithValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseColumnStructuredDataEClass, BaseColumnStructuredData.class, "BaseColumnStructuredData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseColumnStructuredData_Rows(), this.getBaseRowData(), null, "rows", null, 0, -1, BaseColumnStructuredData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBaseColumnStructuredData_Cube(), theData_definitionPackage.getCUBE(), null, "cube", null, 0, 1, BaseColumnStructuredData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseRowDataEClass, BaseRowData.class, "BaseRowData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseRowData_Cells(), this.getBaseCell(), null, "cells", null, 0, -1, BaseRowData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(inputDataEClass, InputData.class, "InputData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getInputData_SourceTableData(), this.getBaseColumnStructuredData(), null, "sourceTableData", null, 0, -1, InputData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, !IS_ORDERED);
		initEAttribute(getInputData_Name(), ecorePackage.getEString(), "name", null, 0, 1, InputData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(inputDataModuleEClass, InputDataModule.class, "InputDataModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getInputDataModule_InputData(), this.getInputData(), null, "inputData", null, 0, -1, InputDataModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (getBaseCellWithValue_Value(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "value"
		   });
		addAnnotation
		  (getBaseColumnStructuredData_Rows(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "rows"
		   });
		addAnnotation
		  (getBaseColumnStructuredData_Cube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getBaseRowData_Cells(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "cells"
		   });
		addAnnotation
		  (getInputData_SourceTableData(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "sourceTableData"
		   });
		addAnnotation
		  (getInputData_Name(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (inputDataModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "InputDataModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getInputDataModule_InputData(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "inputData"
		   });
	}

} //Base_column_structured_dataPackageImpl
