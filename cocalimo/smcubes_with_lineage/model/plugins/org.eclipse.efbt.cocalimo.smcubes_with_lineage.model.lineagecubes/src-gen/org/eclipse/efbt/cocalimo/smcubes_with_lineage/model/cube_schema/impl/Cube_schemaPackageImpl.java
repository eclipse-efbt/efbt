/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl;

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
public class Cube_schemaPackageImpl extends EPackageImpl implements Cube_schemaPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeSchemaEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Cube_schemaPackageImpl()
	{
		super(eNS_URI, Cube_schemaFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Cube_schemaPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Cube_schemaPackage init()
	{
		if (isInited) return (Cube_schemaPackage)EPackage.Registry.INSTANCE.getEPackage(Cube_schemaPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCube_schemaPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Cube_schemaPackageImpl theCube_schemaPackage = registeredCube_schemaPackage instanceof Cube_schemaPackageImpl ? (Cube_schemaPackageImpl)registeredCube_schemaPackage : new Cube_schemaPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		CubesPackageImpl theCubesPackage = (CubesPackageImpl)(registeredPackage instanceof CubesPackageImpl ? registeredPackage : CubesPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);
		Efbt_advanced_data_definitionPackageImpl theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackageImpl)(registeredPackage instanceof Efbt_advanced_data_definitionPackageImpl ? registeredPackage : Efbt_advanced_data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Incremental_cubesPackage.eNS_URI);
		Incremental_cubesPackageImpl theIncremental_cubesPackage = (Incremental_cubesPackageImpl)(registeredPackage instanceof Incremental_cubesPackageImpl ? registeredPackage : Incremental_cubesPackage.eINSTANCE);

		// Create package meta-data objects
		theCube_schemaPackage.createPackageContents();
		theCubesPackage.createPackageContents();
		theEfbt_advanced_data_definitionPackage.createPackageContents();
		theIncremental_cubesPackage.createPackageContents();

		// Initialize created meta-data
		theCube_schemaPackage.initializePackageContents();
		theCubesPackage.initializePackageContents();
		theEfbt_advanced_data_definitionPackage.initializePackageContents();
		theIncremental_cubesPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCube_schemaPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Cube_schemaPackage.eNS_URI, theCube_schemaPackage);
		return theCube_schemaPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeSchema()
	{
		return cubeSchemaEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeSchema_Cube()
	{
		return (EReference)cubeSchemaEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCubeSchema_Name()
	{
		return (EAttribute)cubeSchemaEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeSchema_BaseCube()
	{
		return (EReference)cubeSchemaEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cube_schemaFactory getCube_schemaFactory()
	{
		return (Cube_schemaFactory)getEFactoryInstance();
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
		cubeSchemaEClass = createEClass(CUBE_SCHEMA);
		createEReference(cubeSchemaEClass, CUBE_SCHEMA__CUBE);
		createEAttribute(cubeSchemaEClass, CUBE_SCHEMA__NAME);
		createEReference(cubeSchemaEClass, CUBE_SCHEMA__BASE_CUBE);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(cubeSchemaEClass, CubeSchema.class, "CubeSchema", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeSchema_Cube(), theData_definitionPackage.getCUBE(), null, "cube", null, 0, 1, CubeSchema.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCubeSchema_Name(), ecorePackage.getEString(), "name", null, 0, 1, CubeSchema.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeSchema_BaseCube(), theCubesPackage.getBaseCube(), null, "baseCube", null, 0, 1, CubeSchema.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (cubeSchemaEClass,
		   source,
		   new String[] {
			   "name", "CubeSchema",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCubeSchema_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getCubeSchema_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCubeSchema_BaseCube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "baseCube"
		   });
	}

} //Cube_schemaPackageImpl
