/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.Data_lineage_functionsPackageImpl;

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
public class Advanced_variable_lineagefunctionsPackageImpl extends EPackageImpl implements Advanced_variable_lineagefunctionsPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass getStructColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass resolvedStructColumnParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass speculativeStructColumnParameterEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Advanced_variable_lineagefunctionsPackageImpl()
	{
		super(eNS_URI, Advanced_variable_lineagefunctionsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Advanced_variable_lineagefunctionsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Advanced_variable_lineagefunctionsPackage init()
	{
		if (isInited) return (Advanced_variable_lineagefunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(Advanced_variable_lineagefunctionsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredAdvanced_variable_lineagefunctionsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Advanced_variable_lineagefunctionsPackageImpl theAdvanced_variable_lineagefunctionsPackage = registeredAdvanced_variable_lineagefunctionsPackage instanceof Advanced_variable_lineagefunctionsPackageImpl ? (Advanced_variable_lineagefunctionsPackageImpl)registeredAdvanced_variable_lineagefunctionsPackage : new Advanced_variable_lineagefunctionsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = (Column_transformation_logicPackageImpl)(registeredPackage instanceof Column_transformation_logicPackageImpl ? registeredPackage : Column_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_lineage_functionsPackage.eNS_URI);
		Data_lineage_functionsPackageImpl theData_lineage_functionsPackage = (Data_lineage_functionsPackageImpl)(registeredPackage instanceof Data_lineage_functionsPackageImpl ? registeredPackage : Data_lineage_functionsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Smcubes_functionsPackage.eNS_URI);
		Smcubes_functionsPackageImpl theSmcubes_functionsPackage = (Smcubes_functionsPackageImpl)(registeredPackage instanceof Smcubes_functionsPackageImpl ? registeredPackage : Smcubes_functionsPackage.eINSTANCE);

		// Create package meta-data objects
		theAdvanced_variable_lineagefunctionsPackage.createPackageContents();
		theColumn_transformation_logicPackage.createPackageContents();
		theData_lineage_functionsPackage.createPackageContents();
		theSmcubes_functionsPackage.createPackageContents();

		// Initialize created meta-data
		theAdvanced_variable_lineagefunctionsPackage.initializePackageContents();
		theColumn_transformation_logicPackage.initializePackageContents();
		theData_lineage_functionsPackage.initializePackageContents();
		theSmcubes_functionsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theAdvanced_variable_lineagefunctionsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Advanced_variable_lineagefunctionsPackage.eNS_URI, theAdvanced_variable_lineagefunctionsPackage);
		return theAdvanced_variable_lineagefunctionsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGetStructColumnFunction()
	{
		return getStructColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetStructColumnFunction_ColumnInStruct()
	{
		return (EReference)getStructColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetStructColumnFunction_CubeColumn()
	{
		return (EReference)getStructColumnFunctionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getResolvedStructColumnParameter()
	{
		return resolvedStructColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getResolvedStructColumnParameter_ColumnInsideStruct()
	{
		return (EReference)resolvedStructColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getResolvedStructColumnParameter_CubeColumn()
	{
		return (EReference)resolvedStructColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSpeculativeStructColumnParameter()
	{
		return speculativeStructColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_ColumnInsideStruct()
	{
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_Cube()
	{
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_StructColumn()
	{
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_variable_lineagefunctionsFactory getAdvanced_variable_lineagefunctionsFactory()
	{
		return (Advanced_variable_lineagefunctionsFactory)getEFactoryInstance();
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
		getStructColumnFunctionEClass = createEClass(GET_STRUCT_COLUMN_FUNCTION);
		createEReference(getStructColumnFunctionEClass, GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT);
		createEReference(getStructColumnFunctionEClass, GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN);

		resolvedStructColumnParameterEClass = createEClass(RESOLVED_STRUCT_COLUMN_PARAMETER);
		createEReference(resolvedStructColumnParameterEClass, RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT);
		createEReference(resolvedStructColumnParameterEClass, RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN);

		speculativeStructColumnParameterEClass = createEClass(SPECULATIVE_STRUCT_COLUMN_PARAMETER);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN);
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
		Column_transformation_logicPackage theColumn_transformation_logicPackage = (Column_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		Efbt_advanced_data_definitionPackage theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		getStructColumnFunctionEClass.getESuperTypes().add(theColumn_transformation_logicPackage.getColumnFunction());
		resolvedStructColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		speculativeStructColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(getStructColumnFunctionEClass, GetStructColumnFunction.class, "GetStructColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGetStructColumnFunction_ColumnInStruct(), theCorePackage.getVARIABLE(), null, "columnInStruct", null, 0, 1, GetStructColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetStructColumnFunction_CubeColumn(), theColumn_transformation_logicPackage.getCubeColumn(), null, "cubeColumn", null, 0, 1, GetStructColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(resolvedStructColumnParameterEClass, ResolvedStructColumnParameter.class, "ResolvedStructColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getResolvedStructColumnParameter_ColumnInsideStruct(), theCorePackage.getVARIABLE(), null, "columnInsideStruct", null, 0, 1, ResolvedStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getResolvedStructColumnParameter_CubeColumn(), theColumn_transformation_logicPackage.getCubeColumn(), null, "cubeColumn", null, 0, 1, ResolvedStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(speculativeStructColumnParameterEClass, SpeculativeStructColumnParameter.class, "SpeculativeStructColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSpeculativeStructColumnParameter_ColumnInsideStruct(), theCorePackage.getVARIABLE(), null, "columnInsideStruct", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeStructColumnParameter_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeStructColumnParameter_StructColumn(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "structColumn", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
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

} //Advanced_variable_lineagefunctionsPackageImpl
