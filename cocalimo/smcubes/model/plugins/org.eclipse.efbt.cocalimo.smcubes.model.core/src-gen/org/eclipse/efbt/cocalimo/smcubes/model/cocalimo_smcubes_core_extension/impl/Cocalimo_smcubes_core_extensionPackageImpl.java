/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.Cocalimo_smcubes_core_extensionFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.Cocalimo_smcubes_core_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.MemberHierarchyModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.SMCubesCoreModel;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.SubDomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.VariableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl;
import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

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
public class Cocalimo_smcubes_core_extensionPackageImpl extends EPackageImpl implements Cocalimo_smcubes_core_extensionPackage {
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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subDomainModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smCubesCoreModelEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.Cocalimo_smcubes_core_extensionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Cocalimo_smcubes_core_extensionPackageImpl() {
		super(eNS_URI, Cocalimo_smcubes_core_extensionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Cocalimo_smcubes_core_extensionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Cocalimo_smcubes_core_extensionPackage init() {
		if (isInited) return (Cocalimo_smcubes_core_extensionPackage)EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_core_extensionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCocalimo_smcubes_core_extensionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Cocalimo_smcubes_core_extensionPackageImpl theCocalimo_smcubes_core_extensionPackage = registeredCocalimo_smcubes_core_extensionPackage instanceof Cocalimo_smcubes_core_extensionPackageImpl ? (Cocalimo_smcubes_core_extensionPackageImpl)registeredCocalimo_smcubes_core_extensionPackage : new Cocalimo_smcubes_core_extensionPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_extra_extensionPackage.eNS_URI);
		Cocalimo_smcubes_extra_extensionPackageImpl theCocalimo_smcubes_extra_extensionPackage = (Cocalimo_smcubes_extra_extensionPackageImpl)(registeredPackage instanceof Cocalimo_smcubes_extra_extensionPackageImpl ? registeredPackage : Cocalimo_smcubes_extra_extensionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);

		// Create package meta-data objects
		theCocalimo_smcubes_core_extensionPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theCocalimo_smcubes_extra_extensionPackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();

		// Initialize created meta-data
		theCocalimo_smcubes_core_extensionPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theCocalimo_smcubes_extra_extensionPackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCocalimo_smcubes_core_extensionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Cocalimo_smcubes_core_extensionPackage.eNS_URI, theCocalimo_smcubes_core_extensionPackage);
		return theCocalimo_smcubes_core_extensionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDomainModule() {
		return domainModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainModule_Domains() {
		return (EReference)domainModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberHierarchyModule() {
		return memberHierarchyModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchies() {
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchiesNodes() {
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberModule() {
		return memberModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberModule_Members() {
		return (EReference)memberModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVariableModule() {
		return variableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVariableModule_Variables() {
		return (EReference)variableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSubDomainModule() {
		return subDomainModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSubDomainModule_Subdomains() {
		return (EReference)subDomainModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSMCubesCoreModel() {
		return smCubesCoreModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSMCubesCoreModel_Name() {
		return (EAttribute)smCubesCoreModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cocalimo_smcubes_core_extensionFactory getCocalimo_smcubes_core_extensionFactory() {
		return (Cocalimo_smcubes_core_extensionFactory)getEFactoryInstance();
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
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		domainModuleEClass = createEClass(DOMAIN_MODULE);
		createEReference(domainModuleEClass, DOMAIN_MODULE__DOMAINS);

		memberHierarchyModuleEClass = createEClass(MEMBER_HIERARCHY_MODULE);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES);

		memberModuleEClass = createEClass(MEMBER_MODULE);
		createEReference(memberModuleEClass, MEMBER_MODULE__MEMBERS);

		variableModuleEClass = createEClass(VARIABLE_MODULE);
		createEReference(variableModuleEClass, VARIABLE_MODULE__VARIABLES);

		subDomainModuleEClass = createEClass(SUB_DOMAIN_MODULE);
		createEReference(subDomainModuleEClass, SUB_DOMAIN_MODULE__SUBDOMAINS);

		smCubesCoreModelEClass = createEClass(SM_CUBES_CORE_MODEL);
		createEAttribute(smCubesCoreModelEClass, SM_CUBES_CORE_MODEL__NAME);
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
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		domainModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberHierarchyModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		variableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		subDomainModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(domainModuleEClass, DomainModule.class, "DomainModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDomainModule_Domains(), theCorePackage.getDOMAIN(), null, "domains", null, 0, -1, DomainModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberHierarchyModuleEClass, MemberHierarchyModule.class, "MemberHierarchyModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberHierarchyModule_MemberHierarchies(), theCorePackage.getMEMBER_HIERARCHY(), null, "memberHierarchies", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMemberHierarchyModule_MemberHierarchiesNodes(), theCorePackage.getMEMBER_HIERARCHY_NODE(), null, "memberHierarchiesNodes", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberModuleEClass, MemberModule.class, "MemberModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberModule_Members(), theCorePackage.getMEMBER(), null, "members", null, 0, -1, MemberModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableModuleEClass, VariableModule.class, "VariableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVariableModule_Variables(), theCorePackage.getVARIABLE(), null, "variables", null, 0, -1, VariableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(subDomainModuleEClass, SubDomainModule.class, "SubDomainModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSubDomainModule_Subdomains(), theCorePackage.getSUBDOMAIN(), null, "subdomains", null, 0, -1, SubDomainModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(smCubesCoreModelEClass, SMCubesCoreModel.class, "SMCubesCoreModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getSMCubesCoreModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, SMCubesCoreModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations() {
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
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
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

} //Cocalimo_smcubes_core_extensionPackageImpl
