/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.CubeMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MappingDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MemberMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.VariableMappingModule;

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
public class Efbt_mappingPackageImpl extends EPackageImpl implements Efbt_mappingPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass mappingDefinitionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeMappingModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberMappingModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variableMappingModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Efbt_mappingPackageImpl()
	{
		super(eNS_URI, Efbt_mappingFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Efbt_mappingPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Efbt_mappingPackage init()
	{
		if (isInited) return (Efbt_mappingPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_mappingPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEfbt_mappingPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Efbt_mappingPackageImpl theEfbt_mappingPackage = registeredEfbt_mappingPackage instanceof Efbt_mappingPackageImpl ? (Efbt_mappingPackageImpl)registeredEfbt_mappingPackage : new Efbt_mappingPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_data_definitionPackage.eNS_URI);
		Efbt_data_definitionPackageImpl theEfbt_data_definitionPackage = (Efbt_data_definitionPackageImpl)(registeredPackage instanceof Efbt_data_definitionPackageImpl ? registeredPackage : Efbt_data_definitionPackage.eINSTANCE);
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
		theEfbt_mappingPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theEfbt_data_definitionPackage.createPackageContents();
		theEfbt_vtl_transformationPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theReportsPackage.createPackageContents();
		theSmcubes_modelPackage.createPackageContents();
		theVtl_transformationPackage.createPackageContents();

		// Initialize created meta-data
		theEfbt_mappingPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theEfbt_data_definitionPackage.initializePackageContents();
		theEfbt_vtl_transformationPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theReportsPackage.initializePackageContents();
		theSmcubes_modelPackage.initializePackageContents();
		theVtl_transformationPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEfbt_mappingPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Efbt_mappingPackage.eNS_URI, theEfbt_mappingPackage);
		return theEfbt_mappingPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMappingDefinitionModule()
	{
		return mappingDefinitionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMappingDefinitionModule_Mappings()
	{
		return (EReference)mappingDefinitionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeMappingModule()
	{
		return cubeMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeMappingModule_CubeMappings()
	{
		return (EReference)cubeMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberMappingModule()
	{
		return memberMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberMappingModule_MemberMappings()
	{
		return (EReference)memberMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVariableMappingModule()
	{
		return variableMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVariableMappingModule_VariableMappings()
	{
		return (EReference)variableMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_mappingFactory getEfbt_mappingFactory()
	{
		return (Efbt_mappingFactory)getEFactoryInstance();
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
		mappingDefinitionModuleEClass = createEClass(MAPPING_DEFINITION_MODULE);
		createEReference(mappingDefinitionModuleEClass, MAPPING_DEFINITION_MODULE__MAPPINGS);

		cubeMappingModuleEClass = createEClass(CUBE_MAPPING_MODULE);
		createEReference(cubeMappingModuleEClass, CUBE_MAPPING_MODULE__CUBE_MAPPINGS);

		memberMappingModuleEClass = createEClass(MEMBER_MAPPING_MODULE);
		createEReference(memberMappingModuleEClass, MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS);

		variableMappingModuleEClass = createEClass(VARIABLE_MAPPING_MODULE);
		createEReference(variableMappingModuleEClass, VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS);
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
		MappingPackage theMappingPackage = (MappingPackage)EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		mappingDefinitionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cubeMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		variableMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(mappingDefinitionModuleEClass, MappingDefinitionModule.class, "MappingDefinitionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMappingDefinitionModule_Mappings(), theMappingPackage.getMAPPING_DEFINITION(), null, "mappings", null, 0, -1, MappingDefinitionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeMappingModuleEClass, CubeMappingModule.class, "CubeMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeMappingModule_CubeMappings(), theMappingPackage.getCUBE_MAPPING(), null, "cubeMappings", null, 0, -1, CubeMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberMappingModuleEClass, MemberMappingModule.class, "MemberMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberMappingModule_MemberMappings(), theMappingPackage.getMEMBER_MAPPING(), null, "memberMappings", null, 0, -1, MemberMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableMappingModuleEClass, VariableMappingModule.class, "VariableMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVariableMappingModule_VariableMappings(), theMappingPackage.getVARIABLE_MAPPING(), null, "variableMappings", null, 0, -1, VariableMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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

} //Efbt_mappingPackageImpl
