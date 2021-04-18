/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_moduleFactory;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.SerialFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ValidationFunctionalityModule;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl;

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
public class Functionality_modulePackageImpl extends EPackageImpl implements Functionality_modulePackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataProcessingFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass leafFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass parallelFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass scenarioSetFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass serialFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionalityModuleSetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass readDataSourceFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellCreationFunctionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionalityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionalityModuleModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionalityModuleTagEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass validationFunctionalityModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Functionality_modulePackageImpl()
	{
		super(eNS_URI, Functionality_moduleFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Functionality_modulePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Functionality_modulePackage init()
	{
		if (isInited) return (Functionality_modulePackage)EPackage.Registry.INSTANCE.getEPackage(Functionality_modulePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredFunctionality_modulePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Functionality_modulePackageImpl theFunctionality_modulePackage = registeredFunctionality_modulePackage instanceof Functionality_modulePackageImpl ? (Functionality_modulePackageImpl)registeredFunctionality_modulePackage : new Functionality_modulePackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		ScenariosPackageImpl theScenariosPackage = (ScenariosPackageImpl)(registeredPackage instanceof ScenariosPackageImpl ? registeredPackage : ScenariosPackage.eINSTANCE);

		// Create package meta-data objects
		theFunctionality_modulePackage.createPackageContents();
		theScenariosPackage.createPackageContents();

		// Initialize created meta-data
		theFunctionality_modulePackage.initializePackageContents();
		theScenariosPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theFunctionality_modulePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Functionality_modulePackage.eNS_URI, theFunctionality_modulePackage);
		return theFunctionality_modulePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDataProcessingFunctionalityModule()
	{
		return dataProcessingFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLeafFunctionalityModule()
	{
		return leafFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getParallelFunctionalityModule()
	{
		return parallelFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getScenarioSetFunctionalityModule()
	{
		return scenarioSetFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getScenarioSetFunctionalityModule_ScenarioSetChoice()
	{
		return (EReference)scenarioSetFunctionalityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSerialFunctionalityModule()
	{
		return serialFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFunctionalityModuleSet()
	{
		return functionalityModuleSetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModuleSet_SubModules()
	{
		return (EReference)functionalityModuleSetEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReadDataSourceFunctionalityModule()
	{
		return readDataSourceFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellCreationFunctionalityModule()
	{
		return reportCellCreationFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFunctionalityModule()
	{
		return functionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFunctionalityModule_Name()
	{
		return (EAttribute)functionalityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModule_UsedModule()
	{
		return (EReference)functionalityModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModule_Scenario()
	{
		return (EReference)functionalityModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModule_ScenarioSet()
	{
		return (EReference)functionalityModuleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFunctionalityModuleModule()
	{
		return functionalityModuleModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModuleModule_FunctionalityModules()
	{
		return (EReference)functionalityModuleModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModuleModule_FunctionalityModuleTags()
	{
		return (EReference)functionalityModuleModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModuleModule_SceanrioTags()
	{
		return (EReference)functionalityModuleModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFunctionalityModuleTag()
	{
		return functionalityModuleTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFunctionalityModuleTag_FunctionalityModule()
	{
		return (EReference)functionalityModuleTagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getValidationFunctionalityModule()
	{
		return validationFunctionalityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Functionality_moduleFactory getFunctionality_moduleFactory()
	{
		return (Functionality_moduleFactory)getEFactoryInstance();
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
		dataProcessingFunctionalityModuleEClass = createEClass(DATA_PROCESSING_FUNCTIONALITY_MODULE);

		leafFunctionalityModuleEClass = createEClass(LEAF_FUNCTIONALITY_MODULE);

		parallelFunctionalityModuleEClass = createEClass(PARALLEL_FUNCTIONALITY_MODULE);

		scenarioSetFunctionalityModuleEClass = createEClass(SCENARIO_SET_FUNCTIONALITY_MODULE);
		createEReference(scenarioSetFunctionalityModuleEClass, SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE);

		serialFunctionalityModuleEClass = createEClass(SERIAL_FUNCTIONALITY_MODULE);

		functionalityModuleSetEClass = createEClass(FUNCTIONALITY_MODULE_SET);
		createEReference(functionalityModuleSetEClass, FUNCTIONALITY_MODULE_SET__SUB_MODULES);

		readDataSourceFunctionalityModuleEClass = createEClass(READ_DATA_SOURCE_FUNCTIONALITY_MODULE);

		reportCellCreationFunctionalityModuleEClass = createEClass(REPORT_CELL_CREATION_FUNCTIONALITY_MODULE);

		functionalityModuleEClass = createEClass(FUNCTIONALITY_MODULE);
		createEAttribute(functionalityModuleEClass, FUNCTIONALITY_MODULE__NAME);
		createEReference(functionalityModuleEClass, FUNCTIONALITY_MODULE__USED_MODULE);
		createEReference(functionalityModuleEClass, FUNCTIONALITY_MODULE__SCENARIO);
		createEReference(functionalityModuleEClass, FUNCTIONALITY_MODULE__SCENARIO_SET);

		functionalityModuleModuleEClass = createEClass(FUNCTIONALITY_MODULE_MODULE);
		createEReference(functionalityModuleModuleEClass, FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES);
		createEReference(functionalityModuleModuleEClass, FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS);
		createEReference(functionalityModuleModuleEClass, FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS);

		functionalityModuleTagEClass = createEClass(FUNCTIONALITY_MODULE_TAG);
		createEReference(functionalityModuleTagEClass, FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE);

		validationFunctionalityModuleEClass = createEClass(VALIDATION_FUNCTIONALITY_MODULE);
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
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		dataProcessingFunctionalityModuleEClass.getESuperTypes().add(this.getLeafFunctionalityModule());
		leafFunctionalityModuleEClass.getESuperTypes().add(this.getFunctionalityModule());
		parallelFunctionalityModuleEClass.getESuperTypes().add(this.getFunctionalityModuleSet());
		scenarioSetFunctionalityModuleEClass.getESuperTypes().add(this.getFunctionalityModuleSet());
		serialFunctionalityModuleEClass.getESuperTypes().add(this.getFunctionalityModuleSet());
		functionalityModuleSetEClass.getESuperTypes().add(this.getFunctionalityModule());
		readDataSourceFunctionalityModuleEClass.getESuperTypes().add(this.getLeafFunctionalityModule());
		reportCellCreationFunctionalityModuleEClass.getESuperTypes().add(this.getLeafFunctionalityModule());
		functionalityModuleModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		functionalityModuleTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		validationFunctionalityModuleEClass.getESuperTypes().add(this.getLeafFunctionalityModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(dataProcessingFunctionalityModuleEClass, DataProcessingFunctionalityModule.class, "DataProcessingFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(leafFunctionalityModuleEClass, LeafFunctionalityModule.class, "LeafFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(parallelFunctionalityModuleEClass, ParallelFunctionalityModule.class, "ParallelFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(scenarioSetFunctionalityModuleEClass, ScenarioSetFunctionalityModule.class, "ScenarioSetFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getScenarioSetFunctionalityModule_ScenarioSetChoice(), theScenariosPackage.getScenarioSet(), null, "scenarioSetChoice", null, 0, 1, ScenarioSetFunctionalityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(serialFunctionalityModuleEClass, SerialFunctionalityModule.class, "SerialFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(functionalityModuleSetEClass, FunctionalityModuleSet.class, "FunctionalityModuleSet", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionalityModuleSet_SubModules(), this.getFunctionalityModule(), null, "subModules", null, 0, -1, FunctionalityModuleSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(readDataSourceFunctionalityModuleEClass, ReadDataSourceFunctionalityModule.class, "ReadDataSourceFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(reportCellCreationFunctionalityModuleEClass, ReportCellCreationFunctionalityModule.class, "ReportCellCreationFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(functionalityModuleEClass, FunctionalityModule.class, "FunctionalityModule", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getFunctionalityModule_Name(), ecorePackage.getEString(), "name", null, 0, 1, FunctionalityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFunctionalityModule_UsedModule(), this.getFunctionalityModule(), null, "usedModule", null, 0, 1, FunctionalityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFunctionalityModule_Scenario(), theScenariosPackage.getScenario(), null, "scenario", null, 0, 1, FunctionalityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFunctionalityModule_ScenarioSet(), theScenariosPackage.getScenarioSet(), null, "scenarioSet", null, 0, 1, FunctionalityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionalityModuleModuleEClass, FunctionalityModuleModule.class, "FunctionalityModuleModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionalityModuleModule_FunctionalityModules(), this.getFunctionalityModule(), null, "FunctionalityModules", null, 0, -1, FunctionalityModuleModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFunctionalityModuleModule_FunctionalityModuleTags(), this.getFunctionalityModuleTag(), null, "functionalityModuleTags", null, 0, -1, FunctionalityModuleModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFunctionalityModuleModule_SceanrioTags(), theScenariosPackage.getScenarioTag(), null, "SceanrioTags", null, 0, -1, FunctionalityModuleModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionalityModuleTagEClass, FunctionalityModuleTag.class, "FunctionalityModuleTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionalityModuleTag_FunctionalityModule(), this.getFunctionalityModule(), null, "FunctionalityModule", null, 0, 1, FunctionalityModuleTag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(validationFunctionalityModuleEClass, ValidationFunctionalityModule.class, "ValidationFunctionalityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

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
			   "license", "Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 "
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
		  (leafFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "LeafFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (parallelFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "ParallelFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (scenarioSetFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "ScenarioSetFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getScenarioSetFunctionalityModule_ScenarioSetChoice(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "scenarioSet"
		   });
		addAnnotation
		  (serialFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "SerialFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (functionalityModuleSetEClass,
		   source,
		   new String[] {
			   "name", "FunctionalityModuleSet",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionalityModuleSet_SubModules(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "subModules"
		   });
		addAnnotation
		  (readDataSourceFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "ReadDataSourceFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (reportCellCreationFunctionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "ReportCellCreationFunctionalityModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (functionalityModuleEClass,
		   source,
		   new String[] {
			   "name", "FunctionalityModule",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFunctionalityModule_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getFunctionalityModule_ScenarioSet(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "scenarioSet"
		   });
		addAnnotation
		  (functionalityModuleModuleEClass,
		   source,
		   new String[] {
			   "name", "FunctionalityModuleModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionalityModuleModule_FunctionalityModules(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "FunctionalityModules"
		   });
	}

} //Functionality_modulePackageImpl
