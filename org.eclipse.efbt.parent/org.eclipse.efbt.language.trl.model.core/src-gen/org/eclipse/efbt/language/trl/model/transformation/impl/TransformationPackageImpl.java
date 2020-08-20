/**
 */
package org.eclipse.efbt.language.trl.model.transformation.impl;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.data_structures.smcubes.model.core.CorePackage;

import org.eclipse.efbt.data_structures.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.data_structures.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.data_structures.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.functional_design.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.functional_design.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.Domain_model_mappingPackageImpl;

import org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.trl.model.transformation.Release;
import org.eclipse.efbt.language.trl.model.transformation.TransformationFactory;
import org.eclipse.efbt.language.trl.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.trl.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.impl.Trl_report_cell_viewsPackageImpl;

import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.lineage.common.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

import org.eclipse.efbt.requirements.core.model.requirements_text.Requirements_textPackage;

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
public class TransformationPackageImpl extends EPackageImpl implements TransformationPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataSetTransformationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass releaseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass versionedComponentsSetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass versionedCubeSchemaModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass versionedFunctionalModuleLogicEClass = null;

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
	 * @see org.eclipse.efbt.language.trl.model.transformation.TransformationPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TransformationPackageImpl()
	{
		super(eNS_URI, TransformationFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TransformationPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TransformationPackage init()
	{
		if (isInited) return (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTransformationPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		TransformationPackageImpl theTransformationPackage = registeredTransformationPackage instanceof TransformationPackageImpl ? (TransformationPackageImpl)registeredTransformationPackage : new TransformationPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		LineagefunctionsPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Domain_model_mappingPackage.eNS_URI);
		Domain_model_mappingPackageImpl theDomain_model_mappingPackage = (Domain_model_mappingPackageImpl)(registeredPackage instanceof Domain_model_mappingPackageImpl ? registeredPackage : Domain_model_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Trl_report_cell_viewsPackage.eNS_URI);
		Trl_report_cell_viewsPackageImpl theTrl_report_cell_viewsPackage = (Trl_report_cell_viewsPackageImpl)(registeredPackage instanceof Trl_report_cell_viewsPackageImpl ? registeredPackage : Trl_report_cell_viewsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Trl_sql_viewsPackage.eNS_URI);
		Trl_sql_viewsPackageImpl theTrl_sql_viewsPackage = (Trl_sql_viewsPackageImpl)(registeredPackage instanceof Trl_sql_viewsPackageImpl ? registeredPackage : Trl_sql_viewsPackage.eINSTANCE);

		// Create package meta-data objects
		theTransformationPackage.createPackageContents();
		theDomain_model_mappingPackage.createPackageContents();
		theTrl_report_cell_viewsPackage.createPackageContents();
		theTrl_sql_viewsPackage.createPackageContents();

		// Initialize created meta-data
		theTransformationPackage.initializePackageContents();
		theDomain_model_mappingPackage.initializePackageContents();
		theTrl_report_cell_viewsPackage.initializePackageContents();
		theTrl_sql_viewsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTransformationPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TransformationPackage.eNS_URI, theTransformationPackage);
		return theTransformationPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDataSetTransformation()
	{
		return dataSetTransformationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDataSetTransformation_Cube()
	{
		return (EReference)dataSetTransformationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDataSetTransformation_Name()
	{
		return (EAttribute)dataSetTransformationEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRelease()
	{
		return releaseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getRelease_Version()
	{
		return (EAttribute)releaseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVersionedComponentsSet()
	{
		return versionedComponentsSetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedComponentsSet_CubeSchemaModules()
	{
		return (EReference)versionedComponentsSetEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedComponentsSet_DatasetTransformationModules()
	{
		return (EReference)versionedComponentsSetEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVersionedComponentsSet_Name()
	{
		return (EAttribute)versionedComponentsSetEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedComponentsSet_ReportCellViewModules()
	{
		return (EReference)versionedComponentsSetEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVersionedComponentsSet_License()
	{
		return (EAttribute)versionedComponentsSetEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVersionedCubeSchemaModule()
	{
		return versionedCubeSchemaModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVersionedCubeSchemaModule_Schemas()
	{
		return (EReference)versionedCubeSchemaModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVersionedFunctionalModuleLogic()
	{
		return versionedFunctionalModuleLogicEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TransformationFactory getTransformationFactory()
	{
		return (TransformationFactory)getEFactoryInstance();
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
		dataSetTransformationEClass = createEClass(DATA_SET_TRANSFORMATION);
		createEReference(dataSetTransformationEClass, DATA_SET_TRANSFORMATION__CUBE);
		createEAttribute(dataSetTransformationEClass, DATA_SET_TRANSFORMATION__NAME);

		releaseEClass = createEClass(RELEASE);
		createEAttribute(releaseEClass, RELEASE__VERSION);

		versionedComponentsSetEClass = createEClass(VERSIONED_COMPONENTS_SET);
		createEReference(versionedComponentsSetEClass, VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES);
		createEReference(versionedComponentsSetEClass, VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES);
		createEAttribute(versionedComponentsSetEClass, VERSIONED_COMPONENTS_SET__NAME);
		createEReference(versionedComponentsSetEClass, VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES);
		createEAttribute(versionedComponentsSetEClass, VERSIONED_COMPONENTS_SET__LICENSE);

		versionedCubeSchemaModuleEClass = createEClass(VERSIONED_CUBE_SCHEMA_MODULE);
		createEReference(versionedCubeSchemaModuleEClass, VERSIONED_CUBE_SCHEMA_MODULE__SCHEMAS);

		versionedFunctionalModuleLogicEClass = createEClass(VERSIONED_FUNCTIONAL_MODULE_LOGIC);
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
		Trl_report_cell_viewsPackage theTrl_report_cell_viewsPackage = (Trl_report_cell_viewsPackage)EPackage.Registry.INSTANCE.getEPackage(Trl_report_cell_viewsPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Cube_schemaPackage theCube_schemaPackage = (Cube_schemaPackage)EPackage.Registry.INSTANCE.getEPackage(Cube_schemaPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		releaseEClass.getESuperTypes().add(this.getVersionedComponentsSet());
		versionedCubeSchemaModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		versionedFunctionalModuleLogicEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(dataSetTransformationEClass, DataSetTransformation.class, "DataSetTransformation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDataSetTransformation_Cube(), theCubesPackage.getDerivedCube(), null, "cube", null, 0, 1, DataSetTransformation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDataSetTransformation_Name(), ecorePackage.getEString(), "name", null, 0, 1, DataSetTransformation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(releaseEClass, Release.class, "Release", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRelease_Version(), ecorePackage.getEString(), "version", null, 0, 1, Release.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(versionedComponentsSetEClass, VersionedComponentsSet.class, "VersionedComponentsSet", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVersionedComponentsSet_CubeSchemaModules(), this.getVersionedCubeSchemaModule(), null, "cubeSchemaModules", null, 0, -1, VersionedComponentsSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVersionedComponentsSet_DatasetTransformationModules(), this.getVersionedFunctionalModuleLogic(), null, "datasetTransformationModules", null, 0, -1, VersionedComponentsSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVersionedComponentsSet_Name(), ecorePackage.getEString(), "name", null, 0, 1, VersionedComponentsSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVersionedComponentsSet_ReportCellViewModules(), theTrl_report_cell_viewsPackage.getReportCellViewModule(), null, "reportCellViewModules", null, 0, -1, VersionedComponentsSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVersionedComponentsSet_License(), ecorePackage.getEString(), "license", null, 0, 1, VersionedComponentsSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(versionedCubeSchemaModuleEClass, VersionedCubeSchemaModule.class, "VersionedCubeSchemaModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVersionedCubeSchemaModule_Schemas(), theCube_schemaPackage.getCubeSchema(), null, "schemas", null, 0, -1, VersionedCubeSchemaModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(versionedFunctionalModuleLogicEClass, VersionedFunctionalModuleLogic.class, "VersionedFunctionalModuleLogic", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

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
		  (dataSetTransformationEClass,
		   source,
		   new String[]
		   {
			   "name", "DataSetTransformation",
			   "kind", "empty"
		   });
		addAnnotation
		  (getDataSetTransformation_Cube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getDataSetTransformation_Name(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (releaseEClass,
		   source,
		   new String[]
		   {
			   "name", "Release",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRelease_Version(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (versionedComponentsSetEClass,
		   source,
		   new String[]
		   {
			   "name", "VersionedComponentsSet",
			   "kind", "empty"
		   });
		addAnnotation
		  (getVersionedComponentsSet_CubeSchemaModules(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cubeSchemaModules"
		   });
		addAnnotation
		  (getVersionedComponentsSet_DatasetTransformationModules(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "datasetTransformationModules"
		   });
		addAnnotation
		  (getVersionedComponentsSet_Name(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getVersionedComponentsSet_ReportCellViewModules(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "reportCellViewModules"
		   });
		addAnnotation
		  (versionedCubeSchemaModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "VersionedCubeSchemaModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getVersionedCubeSchemaModule_Schemas(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "schemas"
		   });
		addAnnotation
		  (versionedFunctionalModuleLogicEClass,
		   source,
		   new String[]
		   {
			   "name", "VersionedFunctionalModuleLogic",
			   "kind", "elementOnly"
		   });
	}

} //TransformationPackageImpl
