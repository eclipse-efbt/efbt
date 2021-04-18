/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.Smcubes_functionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl;

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
public class Data_lineage_functionsPackageImpl extends EPackageImpl implements Data_lineage_functionsPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellsParameterEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Data_lineage_functionsPackageImpl()
	{
		super(eNS_URI, Data_lineage_functionsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Data_lineage_functionsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Data_lineage_functionsPackage init()
	{
		if (isInited) return (Data_lineage_functionsPackage)EPackage.Registry.INSTANCE.getEPackage(Data_lineage_functionsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredData_lineage_functionsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Data_lineage_functionsPackageImpl theData_lineage_functionsPackage = registeredData_lineage_functionsPackage instanceof Data_lineage_functionsPackageImpl ? (Data_lineage_functionsPackageImpl)registeredData_lineage_functionsPackage : new Data_lineage_functionsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Advanced_variable_lineagefunctionsPackage.eNS_URI);
		Advanced_variable_lineagefunctionsPackageImpl theAdvanced_variable_lineagefunctionsPackage = (Advanced_variable_lineagefunctionsPackageImpl)(registeredPackage instanceof Advanced_variable_lineagefunctionsPackageImpl ? registeredPackage : Advanced_variable_lineagefunctionsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = (Column_transformation_logicPackageImpl)(registeredPackage instanceof Column_transformation_logicPackageImpl ? registeredPackage : Column_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Smcubes_functionsPackage.eNS_URI);
		Smcubes_functionsPackageImpl theSmcubes_functionsPackage = (Smcubes_functionsPackageImpl)(registeredPackage instanceof Smcubes_functionsPackageImpl ? registeredPackage : Smcubes_functionsPackage.eINSTANCE);

		// Create package meta-data objects
		theData_lineage_functionsPackage.createPackageContents();
		theAdvanced_variable_lineagefunctionsPackage.createPackageContents();
		theColumn_transformation_logicPackage.createPackageContents();
		theSmcubes_functionsPackage.createPackageContents();

		// Initialize created meta-data
		theData_lineage_functionsPackage.initializePackageContents();
		theAdvanced_variable_lineagefunctionsPackage.initializePackageContents();
		theColumn_transformation_logicPackage.initializePackageContents();
		theSmcubes_functionsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theData_lineage_functionsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Data_lineage_functionsPackage.eNS_URI, theData_lineage_functionsPackage);
		return theData_lineage_functionsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCellsParameter()
	{
		return cellsParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCellsParameter_Cells()
	{
		return (EReference)cellsParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineage_functionsFactory getData_lineage_functionsFactory()
	{
		return (Data_lineage_functionsFactory)getEFactoryInstance();
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
		cellsParameterEClass = createEClass(CELLS_PARAMETER);
		createEReference(cellsParameterEClass, CELLS_PARAMETER__CELLS);
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
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		Column_structured_dataPackage theColumn_structured_dataPackage = (Column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Column_structured_dataPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		cellsParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(cellsParameterEClass, CellsParameter.class, "CellsParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCellsParameter_Cells(), theColumn_structured_dataPackage.getCell(), null, "cells", null, 0, 1, CellsParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (getCellsParameter_Cells(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cells"
		   });
	}

} //Data_lineage_functionsPackageImpl
