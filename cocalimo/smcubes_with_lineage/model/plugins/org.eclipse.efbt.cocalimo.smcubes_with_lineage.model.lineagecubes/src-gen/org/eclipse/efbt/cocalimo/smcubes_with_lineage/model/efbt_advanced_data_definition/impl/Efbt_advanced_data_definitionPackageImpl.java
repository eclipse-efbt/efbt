/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.Cube_schemaPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.AdvancedTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl;

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
public class Efbt_advanced_data_definitionPackageImpl extends EPackageImpl implements Efbt_advanced_data_definitionPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass advancedTypedVariableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass arrayTypedVariableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass arrayTypedVariableModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass structTypedVariableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass structTypedVariablesModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Efbt_advanced_data_definitionPackageImpl()
	{
		super(eNS_URI, Efbt_advanced_data_definitionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Efbt_advanced_data_definitionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Efbt_advanced_data_definitionPackage init()
	{
		if (isInited) return (Efbt_advanced_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEfbt_advanced_data_definitionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Efbt_advanced_data_definitionPackageImpl theEfbt_advanced_data_definitionPackage = registeredEfbt_advanced_data_definitionPackage instanceof Efbt_advanced_data_definitionPackageImpl ? (Efbt_advanced_data_definitionPackageImpl)registeredEfbt_advanced_data_definitionPackage : new Efbt_advanced_data_definitionPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cube_schemaPackage.eNS_URI);
		Cube_schemaPackageImpl theCube_schemaPackage = (Cube_schemaPackageImpl)(registeredPackage instanceof Cube_schemaPackageImpl ? registeredPackage : Cube_schemaPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		CubesPackageImpl theCubesPackage = (CubesPackageImpl)(registeredPackage instanceof CubesPackageImpl ? registeredPackage : CubesPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Incremental_cubesPackage.eNS_URI);
		Incremental_cubesPackageImpl theIncremental_cubesPackage = (Incremental_cubesPackageImpl)(registeredPackage instanceof Incremental_cubesPackageImpl ? registeredPackage : Incremental_cubesPackage.eINSTANCE);

		// Create package meta-data objects
		theEfbt_advanced_data_definitionPackage.createPackageContents();
		theCube_schemaPackage.createPackageContents();
		theCubesPackage.createPackageContents();
		theIncremental_cubesPackage.createPackageContents();

		// Initialize created meta-data
		theEfbt_advanced_data_definitionPackage.initializePackageContents();
		theCube_schemaPackage.initializePackageContents();
		theCubesPackage.initializePackageContents();
		theIncremental_cubesPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEfbt_advanced_data_definitionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Efbt_advanced_data_definitionPackage.eNS_URI, theEfbt_advanced_data_definitionPackage);
		return theEfbt_advanced_data_definitionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAdvancedTypedVariable()
	{
		return advancedTypedVariableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getArrayTypedVariable()
	{
		return arrayTypedVariableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getArrayTypedVariable_CubeType()
	{
		return (EReference)arrayTypedVariableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getArrayTypedVariableModule()
	{
		return arrayTypedVariableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getArrayTypedVariableModule_ArrayTypedVariables()
	{
		return (EReference)arrayTypedVariableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getStructTypedVariable()
	{
		return structTypedVariableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getStructTypedVariable_CubeType()
	{
		return (EReference)structTypedVariableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getStructTypedVariablesModule()
	{
		return structTypedVariablesModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getStructTypedVariablesModule_StructTypeVariables()
	{
		return (EReference)structTypedVariablesModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_advanced_data_definitionFactory getEfbt_advanced_data_definitionFactory()
	{
		return (Efbt_advanced_data_definitionFactory)getEFactoryInstance();
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
		advancedTypedVariableEClass = createEClass(ADVANCED_TYPED_VARIABLE);

		arrayTypedVariableEClass = createEClass(ARRAY_TYPED_VARIABLE);
		createEReference(arrayTypedVariableEClass, ARRAY_TYPED_VARIABLE__CUBE_TYPE);

		arrayTypedVariableModuleEClass = createEClass(ARRAY_TYPED_VARIABLE_MODULE);
		createEReference(arrayTypedVariableModuleEClass, ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES);

		structTypedVariableEClass = createEClass(STRUCT_TYPED_VARIABLE);
		createEReference(structTypedVariableEClass, STRUCT_TYPED_VARIABLE__CUBE_TYPE);

		structTypedVariablesModuleEClass = createEClass(STRUCT_TYPED_VARIABLES_MODULE);
		createEReference(structTypedVariablesModuleEClass, STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES);
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
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		advancedTypedVariableEClass.getESuperTypes().add(theCorePackage.getVARIABLE());
		arrayTypedVariableEClass.getESuperTypes().add(this.getAdvancedTypedVariable());
		arrayTypedVariableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		structTypedVariableEClass.getESuperTypes().add(this.getAdvancedTypedVariable());
		structTypedVariablesModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(advancedTypedVariableEClass, AdvancedTypedVariable.class, "AdvancedTypedVariable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(arrayTypedVariableEClass, ArrayTypedVariable.class, "ArrayTypedVariable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getArrayTypedVariable_CubeType(), theCubesPackage.getBaseCube(), null, "cubeType", null, 0, 1, ArrayTypedVariable.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(arrayTypedVariableModuleEClass, ArrayTypedVariableModule.class, "ArrayTypedVariableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getArrayTypedVariableModule_ArrayTypedVariables(), this.getArrayTypedVariable(), null, "arrayTypedVariables", null, 0, -1, ArrayTypedVariableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(structTypedVariableEClass, StructTypedVariable.class, "StructTypedVariable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getStructTypedVariable_CubeType(), theCubesPackage.getBaseCube(), null, "cubeType", null, 0, 1, StructTypedVariable.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(structTypedVariablesModuleEClass, StructTypedVariablesModule.class, "StructTypedVariablesModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getStructTypedVariablesModule_StructTypeVariables(), this.getStructTypedVariable(), null, "structTypeVariables", null, 0, -1, StructTypedVariablesModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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

} //Efbt_advanced_data_definitionPackageImpl
