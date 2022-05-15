/**
 */
package org.eclipse.efbt.openregspecs.model.module_management.impl;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependencies;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleLongName;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementFactory;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.impl.Requirements_textPackageImpl;

import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl;

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
public class Module_managementPackageImpl extends EPackageImpl implements Module_managementPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleLongNameEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleDependenciesEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleDependencyEClass = null;

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
	 * @see org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Module_managementPackageImpl() {
		super(eNS_URI, Module_managementFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Module_managementPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Module_managementPackage init() {
		if (isInited) return (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredModule_managementPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Module_managementPackageImpl theModule_managementPackage = registeredModule_managementPackage instanceof Module_managementPackageImpl ? (Module_managementPackageImpl)registeredModule_managementPackage : new Module_managementPackageImpl();

		isInited = true;

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Requirements_textPackageImpl theRequirements_textPackage = (Requirements_textPackageImpl)(registeredPackage instanceof Requirements_textPackageImpl ? registeredPackage : Requirements_textPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);
		Sql_litePackageImpl theSql_litePackage = (Sql_litePackageImpl)(registeredPackage instanceof Sql_litePackageImpl ? registeredPackage : Sql_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);
		TestingPackageImpl theTestingPackage = (TestingPackageImpl)(registeredPackage instanceof TestingPackageImpl ? registeredPackage : TestingPackage.eINSTANCE);

		// Create package meta-data objects
		theModule_managementPackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();
		theRequirements_textPackage.createPackageContents();
		theSql_litePackage.createPackageContents();
		theTestingPackage.createPackageContents();

		// Initialize created meta-data
		theModule_managementPackage.initializePackageContents();
		theBpmn_litePackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();
		theRequirements_textPackage.initializePackageContents();
		theSql_litePackage.initializePackageContents();
		theTestingPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theModule_managementPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Module_managementPackage.eNS_URI, theModule_managementPackage);
		return theModule_managementPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModule() {
		return moduleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModule_Dependencies() {
		return (EReference)moduleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_TheDescription() {
		return (EAttribute)moduleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_License() {
		return (EAttribute)moduleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_Name() {
		return (EAttribute)moduleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_Version() {
		return (EAttribute)moduleEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModule_LongName() {
		return (EReference)moduleEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModuleLongName() {
		return moduleLongNameEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModuleLongName_Name() {
		return (EAttribute)moduleLongNameEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModuleDependencies() {
		return moduleDependenciesEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModuleDependencies_TheModules() {
		return (EReference)moduleDependenciesEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModuleDependency() {
		return moduleDependencyEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModuleDependency_ModuleName() {
		return (EAttribute)moduleDependencyEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModuleDependency_ModuleVersion() {
		return (EAttribute)moduleDependencyEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModuleDependency_TheModule() {
		return (EReference)moduleDependencyEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModuleDependency_LongName() {
		return (EReference)moduleDependencyEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Module_managementFactory getModule_managementFactory() {
		return (Module_managementFactory)getEFactoryInstance();
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
		moduleEClass = createEClass(MODULE);
		createEReference(moduleEClass, MODULE__DEPENDENCIES);
		createEAttribute(moduleEClass, MODULE__THE_DESCRIPTION);
		createEAttribute(moduleEClass, MODULE__LICENSE);
		createEAttribute(moduleEClass, MODULE__NAME);
		createEAttribute(moduleEClass, MODULE__VERSION);
		createEReference(moduleEClass, MODULE__LONG_NAME);

		moduleLongNameEClass = createEClass(MODULE_LONG_NAME);
		createEAttribute(moduleLongNameEClass, MODULE_LONG_NAME__NAME);

		moduleDependenciesEClass = createEClass(MODULE_DEPENDENCIES);
		createEReference(moduleDependenciesEClass, MODULE_DEPENDENCIES__THE_MODULES);

		moduleDependencyEClass = createEClass(MODULE_DEPENDENCY);
		createEAttribute(moduleDependencyEClass, MODULE_DEPENDENCY__MODULE_NAME);
		createEAttribute(moduleDependencyEClass, MODULE_DEPENDENCY__MODULE_VERSION);
		createEReference(moduleDependencyEClass, MODULE_DEPENDENCY__THE_MODULE);
		createEReference(moduleDependencyEClass, MODULE_DEPENDENCY__LONG_NAME);
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

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(moduleEClass, org.eclipse.efbt.openregspecs.model.module_management.Module.class, "Module", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getModule_Dependencies(), this.getModuleDependencies(), null, "dependencies", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getModule_TheDescription(), ecorePackage.getEString(), "theDescription", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getModule_License(), ecorePackage.getEString(), "license", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getModule_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getModule_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getModule_LongName(), this.getModuleLongName(), null, "longName", null, 0, 1, org.eclipse.efbt.openregspecs.model.module_management.Module.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(moduleLongNameEClass, ModuleLongName.class, "ModuleLongName", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getModuleLongName_Name(), ecorePackage.getEString(), "name", null, 0, 1, ModuleLongName.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(moduleDependenciesEClass, ModuleDependencies.class, "ModuleDependencies", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getModuleDependencies_TheModules(), this.getModuleDependency(), null, "theModules", null, 0, -1, ModuleDependencies.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(moduleDependencyEClass, ModuleDependency.class, "ModuleDependency", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getModuleDependency_ModuleName(), ecorePackage.getEString(), "moduleName", null, 0, 1, ModuleDependency.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getModuleDependency_ModuleVersion(), ecorePackage.getEString(), "moduleVersion", null, 0, 1, ModuleDependency.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getModuleDependency_TheModule(), this.getModule(), null, "theModule", null, 0, 1, ModuleDependency.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getModuleDependency_LongName(), this.getModuleLongName(), null, "longName", null, 0, 1, ModuleDependency.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (moduleEClass,
		   source,
		   new String[] {
			   "name", "Module",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getModule_Dependencies(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "dependencies"
		   });
		addAnnotation
		  (getModule_TheDescription(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "theDescription"
		   });
		addAnnotation
		  (getModule_License(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "license"
		   });
		addAnnotation
		  (getModule_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getModule_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (moduleDependenciesEClass,
		   source,
		   new String[] {
			   "name", "ModuleDependencies",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getModuleDependencies_TheModules(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "theModules"
		   });
		addAnnotation
		  (moduleDependencyEClass,
		   source,
		   new String[] {
			   "name", "ModuleDependency",
			   "kind", "empty"
		   });
		addAnnotation
		  (getModuleDependency_ModuleName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "moduleName"
		   });
		addAnnotation
		  (getModuleDependency_ModuleVersion(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "moduleVersion"
		   });
		addAnnotation
		  (getModuleDependency_TheModule(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "theModule"
		   });
	}

} //Module_managementPackageImpl
