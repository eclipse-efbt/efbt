/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl.Efbt_mappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.impl.Efbt_vtl_transformationPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportsPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl;

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
public class Efbt_data_definitionPackageImpl extends EPackageImpl implements Efbt_data_definitionPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass combinationModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass domainModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberHierarchyModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variableModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Efbt_data_definitionPackageImpl()
	{
		super(eNS_URI, Efbt_data_definitionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Efbt_data_definitionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Efbt_data_definitionPackage init()
	{
		if (isInited) return (Efbt_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_data_definitionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEfbt_data_definitionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Efbt_data_definitionPackageImpl theEfbt_data_definitionPackage = registeredEfbt_data_definitionPackage instanceof Efbt_data_definitionPackageImpl ? (Efbt_data_definitionPackageImpl)registeredEfbt_data_definitionPackage : new Efbt_data_definitionPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_mappingPackage.eNS_URI);
		Efbt_mappingPackageImpl theEfbt_mappingPackage = (Efbt_mappingPackageImpl)(registeredPackage instanceof Efbt_mappingPackageImpl ? registeredPackage : Efbt_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_vtl_transformationPackage.eNS_URI);
		Efbt_vtl_transformationPackageImpl theEfbt_vtl_transformationPackage = (Efbt_vtl_transformationPackageImpl)(registeredPackage instanceof Efbt_vtl_transformationPackageImpl ? registeredPackage : Efbt_vtl_transformationPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);
		ReportsPackageImpl theReportsPackage = (ReportsPackageImpl)(registeredPackage instanceof ReportsPackageImpl ? registeredPackage : ReportsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Smcubes_modelPackage.eNS_URI);
		Smcubes_modelPackageImpl theSmcubes_modelPackage = (Smcubes_modelPackageImpl)(registeredPackage instanceof Smcubes_modelPackageImpl ? registeredPackage : Smcubes_modelPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Vtl_transformationPackage.eNS_URI);
		Vtl_transformationPackageImpl theVtl_transformationPackage = (Vtl_transformationPackageImpl)(registeredPackage instanceof Vtl_transformationPackageImpl ? registeredPackage : Vtl_transformationPackage.eINSTANCE);

		// Create package meta-data objects
		theEfbt_data_definitionPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theEfbt_mappingPackage.createPackageContents();
		theEfbt_vtl_transformationPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theReportsPackage.createPackageContents();
		theSmcubes_modelPackage.createPackageContents();
		theVtl_transformationPackage.createPackageContents();

		// Initialize created meta-data
		theEfbt_data_definitionPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theEfbt_mappingPackage.initializePackageContents();
		theEfbt_vtl_transformationPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theReportsPackage.initializePackageContents();
		theSmcubes_modelPackage.initializePackageContents();
		theVtl_transformationPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEfbt_data_definitionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Efbt_data_definitionPackage.eNS_URI, theEfbt_data_definitionPackage);
		return theEfbt_data_definitionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCombinationModule()
	{
		return combinationModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_Combinations()
	{
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_Combination_items()
	{
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_CubeToCombinationLinks()
	{
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeModule()
	{
		return cubeModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_Cubes()
	{
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_CubeStructures()
	{
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_CubeStructureItems()
	{
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDomainModule()
	{
		return domainModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainModule_Domains()
	{
		return (EReference)domainModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberHierarchyModule()
	{
		return memberHierarchyModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchies()
	{
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchiesNodes()
	{
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberModule()
	{
		return memberModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberModule_Members()
	{
		return (EReference)memberModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVariableModule()
	{
		return variableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVariableModule_Variables()
	{
		return (EReference)variableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_data_definitionFactory getEfbt_data_definitionFactory()
	{
		return (Efbt_data_definitionFactory)getEFactoryInstance();
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
		combinationModuleEClass = createEClass(COMBINATION_MODULE);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__COMBINATIONS);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__COMBINATION_ITEMS);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS);

		cubeModuleEClass = createEClass(CUBE_MODULE);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBES);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBE_STRUCTURES);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBE_STRUCTURE_ITEMS);

		domainModuleEClass = createEClass(DOMAIN_MODULE);
		createEReference(domainModuleEClass, DOMAIN_MODULE__DOMAINS);

		memberHierarchyModuleEClass = createEClass(MEMBER_HIERARCHY_MODULE);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES);

		memberModuleEClass = createEClass(MEMBER_MODULE);
		createEReference(memberModuleEClass, MEMBER_MODULE__MEMBERS);

		variableModuleEClass = createEClass(VARIABLE_MODULE);
		createEReference(variableModuleEClass, VARIABLE_MODULE__VARIABLES);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		combinationModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cubeModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		domainModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberHierarchyModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		variableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(combinationModuleEClass, CombinationModule.class, "CombinationModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCombinationModule_Combinations(), theData_definitionPackage.getCOMBINATION(), null, "combinations", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCombinationModule_Combination_items(), theData_definitionPackage.getCOMBINATION_ITEM(), null, "combination_items", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCombinationModule_CubeToCombinationLinks(), theData_definitionPackage.getCUBE_TO_COMBINATION(), null, "cubeToCombinationLinks", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeModuleEClass, CubeModule.class, "CubeModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeModule_Cubes(), theData_definitionPackage.getCUBE(), null, "cubes", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeModule_CubeStructures(), theData_definitionPackage.getCUBE_STRUCTURE(), null, "cubeStructures", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeModule_CubeStructureItems(), theData_definitionPackage.getCUBE_STRUCTURE_ITEM(), null, "cubeStructureItems", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(domainModuleEClass, DomainModule.class, "DomainModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDomainModule_Domains(), theCorePackage.getDOMAIN(), null, "domains", null, 0, -1, DomainModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberHierarchyModuleEClass, MemberHierarchyModule.class, "MemberHierarchyModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberHierarchyModule_MemberHierarchies(), theCorePackage.getMEMBER_HIERARCHY(), null, "memberHierarchies", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMemberHierarchyModule_MemberHierarchiesNodes(), theCorePackage.getMEMBER_HIERARCHY_NODE(), null, "memberHierarchiesNodes", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberModuleEClass, MemberModule.class, "MemberModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberModule_Members(), theCorePackage.getMEMBER(), null, "members", null, 0, -1, MemberModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableModuleEClass, VariableModule.class, "VariableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVariableModule_Variables(), theCorePackage.getVARIABLE(), null, "variables", null, 0, -1, VariableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (combinationModuleEClass,
		   source,
		   new String[] {
			   "name", "CombinationModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCombinationModule_Combinations(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "combinations"
		   });
		addAnnotation
		  (getCombinationModule_Combination_items(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "combination_items"
		   });
		addAnnotation
		  (getCombinationModule_CubeToCombinationLinks(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeToCombinationLinks"
		   });
		addAnnotation
		  (cubeModuleEClass,
		   source,
		   new String[] {
			   "name", "CubeModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCubeModule_Cubes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubes"
		   });
		addAnnotation
		  (getCubeModule_CubeStructures(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeStructures"
		   });
		addAnnotation
		  (getCubeModule_CubeStructureItems(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeStructureItems"
		   });
		addAnnotation
		  (domainModuleEClass,
		   source,
		   new String[] {
			   "name", "DomainModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getDomainModule_Domains(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "domains"
		   });
		addAnnotation
		  (memberHierarchyModuleEClass,
		   source,
		   new String[] {
			   "name", "MemberHierarchyModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getMemberHierarchyModule_MemberHierarchies(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "memberHierarchies"
		   });
		addAnnotation
		  (getMemberHierarchyModule_MemberHierarchiesNodes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "memberHierarchiesNodes"
		   });
		addAnnotation
		  (memberModuleEClass,
		   source,
		   new String[] {
			   "name", "MemberModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getMemberModule_Members(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "members"
		   });
		addAnnotation
		  (variableModuleEClass,
		   source,
		   new String[] {
			   "name", "VariableModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getVariableModule_Variables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "variables"
		   });
	}

} //Efbt_data_definitionPackageImpl
