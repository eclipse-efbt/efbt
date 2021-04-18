/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineageFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineagePackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

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
public class Data_lineagePackageImpl extends EPackageImpl implements Data_lineagePackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataLineageModelEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineagePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Data_lineagePackageImpl()
	{
		super(eNS_URI, Data_lineageFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Data_lineagePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Data_lineagePackage init()
	{
		if (isInited) return (Data_lineagePackage)EPackage.Registry.INSTANCE.getEPackage(Data_lineagePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredData_lineagePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Data_lineagePackageImpl theData_lineagePackage = registeredData_lineagePackage instanceof Data_lineagePackageImpl ? (Data_lineagePackageImpl)registeredData_lineagePackage : new Data_lineagePackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cell_transformation_logicPackage.eNS_URI);
		Cell_transformation_logicPackageImpl theCell_transformation_logicPackage = (Cell_transformation_logicPackageImpl)(registeredPackage instanceof Cell_transformation_logicPackageImpl ? registeredPackage : Cell_transformation_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theData_lineagePackage.createPackageContents();
		theCell_transformation_logicPackage.createPackageContents();

		// Initialize created meta-data
		theData_lineagePackage.initializePackageContents();
		theCell_transformation_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theData_lineagePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Data_lineagePackage.eNS_URI, theData_lineagePackage);
		return theData_lineagePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDataLineageModel()
	{
		return dataLineageModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDataLineageModel_DataTraceableByCell()
	{
		return (EReference)dataLineageModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDataLineageModel_SourceData()
	{
		return (EReference)dataLineageModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getDataLineageModel_Name()
	{
		return (EAttribute)dataLineageModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineageFactory getData_lineageFactory()
	{
		return (Data_lineageFactory)getEFactoryInstance();
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
		dataLineageModelEClass = createEClass(DATA_LINEAGE_MODEL);
		createEReference(dataLineageModelEClass, DATA_LINEAGE_MODEL__DATA_TRACEABLE_BY_CELL);
		createEReference(dataLineageModelEClass, DATA_LINEAGE_MODEL__SOURCE_DATA);
		createEAttribute(dataLineageModelEClass, DATA_LINEAGE_MODEL__NAME);
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
		Cell_transformation_logicPackage theCell_transformation_logicPackage = (Cell_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Cell_transformation_logicPackage.eNS_URI);
		Base_column_structured_dataPackage theBase_column_structured_dataPackage = (Base_column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Base_column_structured_dataPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(dataLineageModelEClass, DataLineageModel.class, "DataLineageModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDataLineageModel_DataTraceableByCell(), theCell_transformation_logicPackage.getDerivedCubeData(), null, "dataTraceableByCell", null, 0, -1, DataLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDataLineageModel_SourceData(), theBase_column_structured_dataPackage.getBaseColumnStructuredData(), null, "sourceData", null, 0, -1, DataLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDataLineageModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, DataLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (dataLineageModelEClass,
		   source,
		   new String[] {
			   "name", "DataLineageModel",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getDataLineageModel_DataTraceableByCell(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "dataTraceableByCell"
		   });
		addAnnotation
		  (getDataLineageModel_SourceData(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "sourceData"
		   });
		addAnnotation
		  (getDataLineageModel_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
	}

} //Data_lineagePackageImpl
