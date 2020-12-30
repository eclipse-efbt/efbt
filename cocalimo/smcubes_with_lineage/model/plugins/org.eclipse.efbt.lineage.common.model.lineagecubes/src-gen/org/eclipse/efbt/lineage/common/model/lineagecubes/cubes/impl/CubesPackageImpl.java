/**
 */
package org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.impl;

import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.lineage.common.model.lineagecubes.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.lineage.common.model.lineagecubes.cube_schema.impl.Cube_schemaPackageImpl;

import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.BaseCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.CubesFactory;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.CubesPackage;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.DerivedCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBIRDToolsCubeHierarchyRelationship;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCubeModule;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.TargetCube;

import org.eclipse.efbt.lineage.common.model.lineagecubes.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.lineage.common.model.lineagecubes.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl;

import org.eclipse.efbt.lineage.common.model.lineagecubes.incremental_cubes.Incremental_cubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagecubes.incremental_cubes.impl.Incremental_cubesPackageImpl;

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
public class CubesPackageImpl extends EPackageImpl implements CubesPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass freeBirdToolsCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass freeBirdToolsCubeModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass targetCubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass freeBIRDToolsCubeHierarchyRelationshipModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass freeBIRDToolsCubeHierarchyRelationshipEClass = null;

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
	 * @see org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.CubesPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private CubesPackageImpl()
	{
		super(eNS_URI, CubesFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link CubesPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static CubesPackage init()
	{
		if (isInited) return (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCubesPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		CubesPackageImpl theCubesPackage = registeredCubesPackage instanceof CubesPackageImpl ? (CubesPackageImpl)registeredCubesPackage : new CubesPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cube_schemaPackage.eNS_URI);
		Cube_schemaPackageImpl theCube_schemaPackage = (Cube_schemaPackageImpl)(registeredPackage instanceof Cube_schemaPackageImpl ? registeredPackage : Cube_schemaPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);
		Efbt_advanced_data_definitionPackageImpl theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackageImpl)(registeredPackage instanceof Efbt_advanced_data_definitionPackageImpl ? registeredPackage : Efbt_advanced_data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Incremental_cubesPackage.eNS_URI);
		Incremental_cubesPackageImpl theIncremental_cubesPackage = (Incremental_cubesPackageImpl)(registeredPackage instanceof Incremental_cubesPackageImpl ? registeredPackage : Incremental_cubesPackage.eINSTANCE);

		// Create package meta-data objects
		theCubesPackage.createPackageContents();
		theCube_schemaPackage.createPackageContents();
		theEfbt_advanced_data_definitionPackage.createPackageContents();
		theIncremental_cubesPackage.createPackageContents();

		// Initialize created meta-data
		theCubesPackage.initializePackageContents();
		theCube_schemaPackage.initializePackageContents();
		theEfbt_advanced_data_definitionPackage.initializePackageContents();
		theIncremental_cubesPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCubesPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(CubesPackage.eNS_URI, theCubesPackage);
		return theCubesPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBaseCube()
	{
		return baseCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDerivedCube()
	{
		return derivedCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDerivedCube_SourceCubes()
	{
		return (EReference)derivedCubeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFreeBirdToolsCube()
	{
		return freeBirdToolsCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFreeBirdToolsCube_Name()
	{
		return (EAttribute)freeBirdToolsCubeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFreeBirdToolsCubeModule()
	{
		return freeBirdToolsCubeModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFreeBirdToolsCubeModule_Cubes()
	{
		return (EReference)freeBirdToolsCubeModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTargetCube()
	{
		return targetCubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFreeBIRDToolsCubeHierarchyRelationshipModule()
	{
		return freeBIRDToolsCubeHierarchyRelationshipModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFreeBIRDToolsCubeHierarchyRelationshipModule_HierarchyRelationships()
	{
		return (EReference)freeBIRDToolsCubeHierarchyRelationshipModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFreeBIRDToolsCubeHierarchyRelationship()
	{
		return freeBIRDToolsCubeHierarchyRelationshipEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFreeBIRDToolsCubeHierarchyRelationship_SuperCube()
	{
		return (EReference)freeBIRDToolsCubeHierarchyRelationshipEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFreeBIRDToolsCubeHierarchyRelationship_SubCube()
	{
		return (EReference)freeBIRDToolsCubeHierarchyRelationshipEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubesFactory getCubesFactory()
	{
		return (CubesFactory)getEFactoryInstance();
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
		baseCubeEClass = createEClass(BASE_CUBE);

		derivedCubeEClass = createEClass(DERIVED_CUBE);
		createEReference(derivedCubeEClass, DERIVED_CUBE__SOURCE_CUBES);

		freeBirdToolsCubeEClass = createEClass(FREE_BIRD_TOOLS_CUBE);
		createEAttribute(freeBirdToolsCubeEClass, FREE_BIRD_TOOLS_CUBE__NAME);

		freeBirdToolsCubeModuleEClass = createEClass(FREE_BIRD_TOOLS_CUBE_MODULE);
		createEReference(freeBirdToolsCubeModuleEClass, FREE_BIRD_TOOLS_CUBE_MODULE__CUBES);

		targetCubeEClass = createEClass(TARGET_CUBE);

		freeBIRDToolsCubeHierarchyRelationshipModuleEClass = createEClass(FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE);
		createEReference(freeBIRDToolsCubeHierarchyRelationshipModuleEClass, FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS);

		freeBIRDToolsCubeHierarchyRelationshipEClass = createEClass(FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP);
		createEReference(freeBIRDToolsCubeHierarchyRelationshipEClass, FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE);
		createEReference(freeBIRDToolsCubeHierarchyRelationshipEClass, FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE);
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
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		baseCubeEClass.getESuperTypes().add(this.getFreeBirdToolsCube());
		derivedCubeEClass.getESuperTypes().add(this.getFreeBirdToolsCube());
		freeBirdToolsCubeModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		targetCubeEClass.getESuperTypes().add(this.getDerivedCube());
		freeBIRDToolsCubeHierarchyRelationshipModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(baseCubeEClass, BaseCube.class, "BaseCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(derivedCubeEClass, DerivedCube.class, "DerivedCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedCube_SourceCubes(), this.getFreeBirdToolsCube(), null, "sourceCubes", null, 0, -1, DerivedCube.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(freeBirdToolsCubeEClass, FreeBirdToolsCube.class, "FreeBirdToolsCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getFreeBirdToolsCube_Name(), ecorePackage.getEString(), "name", null, 0, 1, FreeBirdToolsCube.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(freeBirdToolsCubeModuleEClass, FreeBirdToolsCubeModule.class, "FreeBirdToolsCubeModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFreeBirdToolsCubeModule_Cubes(), this.getFreeBirdToolsCube(), null, "cubes", null, 0, -1, FreeBirdToolsCubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(targetCubeEClass, TargetCube.class, "TargetCube", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(freeBIRDToolsCubeHierarchyRelationshipModuleEClass, FreeBIRDToolsCubeHierarchyRelationshipModule.class, "FreeBIRDToolsCubeHierarchyRelationshipModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFreeBIRDToolsCubeHierarchyRelationshipModule_HierarchyRelationships(), this.getFreeBIRDToolsCubeHierarchyRelationship(), null, "hierarchyRelationships", null, 0, -1, FreeBIRDToolsCubeHierarchyRelationshipModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(freeBIRDToolsCubeHierarchyRelationshipEClass, FreeBIRDToolsCubeHierarchyRelationship.class, "FreeBIRDToolsCubeHierarchyRelationship", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFreeBIRDToolsCubeHierarchyRelationship_SuperCube(), this.getFreeBirdToolsCube(), null, "superCube", null, 0, 1, FreeBIRDToolsCubeHierarchyRelationship.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFreeBIRDToolsCubeHierarchyRelationship_SubCube(), this.getFreeBirdToolsCube(), null, "subCube", null, 0, 1, FreeBIRDToolsCubeHierarchyRelationship.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r\r\nCopyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
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
		  (baseCubeEClass,
		   source,
		   new String[]
		   {
			   "name", "BaseCube",
			   "kind", "empty"
		   });
		addAnnotation
		  (derivedCubeEClass,
		   source,
		   new String[]
		   {
			   "name", "DerivedCube",
			   "kind", "empty"
		   });
		addAnnotation
		  (getDerivedCube_SourceCubes(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "sourceCubes"
		   });
		addAnnotation
		  (freeBirdToolsCubeEClass,
		   source,
		   new String[]
		   {
			   "name", "FreeBirdToolsCube",
			   "kind", "empty"
		   });
		addAnnotation
		  (freeBirdToolsCubeModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "FreeBirdToolsCubeModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFreeBirdToolsCubeModule_Cubes(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "cubes"
		   });
		addAnnotation
		  (targetCubeEClass,
		   source,
		   new String[]
		   {
			   "name", "TargetCube",
			   "kind", "empty"
		   });
	}

} //CubesPackageImpl
