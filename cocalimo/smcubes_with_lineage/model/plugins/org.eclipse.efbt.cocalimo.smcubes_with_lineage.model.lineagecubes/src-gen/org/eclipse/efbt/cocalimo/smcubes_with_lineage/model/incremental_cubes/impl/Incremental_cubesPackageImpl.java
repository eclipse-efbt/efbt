/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.Cube_schemaPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.DeltaAccumulationCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage;

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
public class Incremental_cubesPackageImpl extends EPackageImpl implements Incremental_cubesPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseDeltaCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseViewCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass deltaAccumulationCubeEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Incremental_cubesPackageImpl()
	{
		super(eNS_URI, Incremental_cubesFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Incremental_cubesPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Incremental_cubesPackage init()
	{
		if (isInited) return (Incremental_cubesPackage)EPackage.Registry.INSTANCE.getEPackage(Incremental_cubesPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredIncremental_cubesPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Incremental_cubesPackageImpl theIncremental_cubesPackage = registeredIncremental_cubesPackage instanceof Incremental_cubesPackageImpl ? (Incremental_cubesPackageImpl)registeredIncremental_cubesPackage : new Incremental_cubesPackageImpl();

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
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);
		Efbt_advanced_data_definitionPackageImpl theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackageImpl)(registeredPackage instanceof Efbt_advanced_data_definitionPackageImpl ? registeredPackage : Efbt_advanced_data_definitionPackage.eINSTANCE);

		// Create package meta-data objects
		theIncremental_cubesPackage.createPackageContents();
		theCube_schemaPackage.createPackageContents();
		theCubesPackage.createPackageContents();
		theEfbt_advanced_data_definitionPackage.createPackageContents();

		// Initialize created meta-data
		theIncremental_cubesPackage.initializePackageContents();
		theCube_schemaPackage.initializePackageContents();
		theCubesPackage.initializePackageContents();
		theEfbt_advanced_data_definitionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theIncremental_cubesPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Incremental_cubesPackage.eNS_URI, theIncremental_cubesPackage);
		return theIncremental_cubesPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseDeltaCube()
	{
		return baseDeltaCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseDeltaCube_UltimateBaseCube()
	{
		return (EReference)baseDeltaCubeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseViewCube()
	{
		return baseViewCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBaseViewCube_UltimateBaseCube()
	{
		return (EReference)baseViewCubeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDeltaAccumulationCube()
	{
		return deltaAccumulationCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Incremental_cubesFactory getIncremental_cubesFactory()
	{
		return (Incremental_cubesFactory)getEFactoryInstance();
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
		baseDeltaCubeEClass = createEClass(BASE_DELTA_CUBE);
		createEReference(baseDeltaCubeEClass, BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE);

		baseViewCubeEClass = createEClass(BASE_VIEW_CUBE);
		createEReference(baseViewCubeEClass, BASE_VIEW_CUBE__ULTIMATE_BASE_CUBE);

		deltaAccumulationCubeEClass = createEClass(DELTA_ACCUMULATION_CUBE);
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
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		baseDeltaCubeEClass.getESuperTypes().add(theCubesPackage.getBaseCube());
		baseViewCubeEClass.getESuperTypes().add(theCubesPackage.getDerivedCube());
		deltaAccumulationCubeEClass.getESuperTypes().add(theCubesPackage.getDerivedCube());

		// Initialize classes, features, and operations; add parameters
		initEClass(baseDeltaCubeEClass, BaseDeltaCube.class, "BaseDeltaCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseDeltaCube_UltimateBaseCube(), theCubesPackage.getBaseCube(), null, "ultimateBaseCube", null, 0, 1, BaseDeltaCube.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseViewCubeEClass, BaseViewCube.class, "BaseViewCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseViewCube_UltimateBaseCube(), theCubesPackage.getBaseCube(), null, "ultimateBaseCube", null, 0, 1, BaseViewCube.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(deltaAccumulationCubeEClass, DeltaAccumulationCube.class, "DeltaAccumulationCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r\r\nCopyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

} //Incremental_cubesPackageImpl
