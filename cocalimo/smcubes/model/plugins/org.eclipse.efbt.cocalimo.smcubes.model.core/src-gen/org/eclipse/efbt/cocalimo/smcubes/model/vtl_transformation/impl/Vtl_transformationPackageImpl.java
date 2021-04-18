/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl;

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

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Vtl_transformationPackageImpl extends EPackageImpl implements Vtl_transformationPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass transformatioN_SCHEMEEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass transformationEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Vtl_transformationPackageImpl()
	{
		super(eNS_URI, Vtl_transformationFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Vtl_transformationPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Vtl_transformationPackage init()
	{
		if (isInited) return (Vtl_transformationPackage)EPackage.Registry.INSTANCE.getEPackage(Vtl_transformationPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredVtl_transformationPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Vtl_transformationPackageImpl theVtl_transformationPackage = registeredVtl_transformationPackage instanceof Vtl_transformationPackageImpl ? (Vtl_transformationPackageImpl)registeredVtl_transformationPackage : new Vtl_transformationPackageImpl();

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

		// Create package meta-data objects
		theVtl_transformationPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theEfbt_data_definitionPackage.createPackageContents();
		theEfbt_mappingPackage.createPackageContents();
		theEfbt_vtl_transformationPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theReportsPackage.createPackageContents();
		theSmcubes_modelPackage.createPackageContents();

		// Initialize created meta-data
		theVtl_transformationPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theEfbt_data_definitionPackage.initializePackageContents();
		theEfbt_mappingPackage.initializePackageContents();
		theEfbt_vtl_transformationPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theReportsPackage.initializePackageContents();
		theSmcubes_modelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theVtl_transformationPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Vtl_transformationPackage.eNS_URI, theVtl_transformationPackage);
		return theVtl_transformationPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTRANSFORMATION_SCHEME()
	{
		return transformatioN_SCHEMEEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Transformation_scheme_id()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Description()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Name()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Code()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Version()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Valid_to()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_Valid_from()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_SCHEME_DisplayName()
	{
		return (EAttribute)transformatioN_SCHEMEEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTRANSFORMATION()
	{
		return transformationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Transformation_id()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Description()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Expression()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Name()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Code()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_Maintenance_agency_id()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTRANSFORMATION_DisplayName()
	{
		return (EAttribute)transformationEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Vtl_transformationFactory getVtl_transformationFactory()
	{
		return (Vtl_transformationFactory)getEFactoryInstance();
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
		transformatioN_SCHEMEEClass = createEClass(TRANSFORMATION_SCHEME);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__DESCRIPTION);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__NAME);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__CODE);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__VERSION);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__VALID_TO);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__VALID_FROM);
		createEAttribute(transformatioN_SCHEMEEClass, TRANSFORMATION_SCHEME__DISPLAY_NAME);

		transformationEClass = createEClass(TRANSFORMATION);
		createEAttribute(transformationEClass, TRANSFORMATION__TRANSFORMATION_ID);
		createEAttribute(transformationEClass, TRANSFORMATION__DESCRIPTION);
		createEAttribute(transformationEClass, TRANSFORMATION__EXPRESSION);
		createEAttribute(transformationEClass, TRANSFORMATION__NAME);
		createEAttribute(transformationEClass, TRANSFORMATION__CODE);
		createEAttribute(transformationEClass, TRANSFORMATION__MAINTENANCE_AGENCY_ID);
		createEAttribute(transformationEClass, TRANSFORMATION__DISPLAY_NAME);
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

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(transformatioN_SCHEMEEClass, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, "TRANSFORMATION_SCHEME", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTRANSFORMATION_SCHEME_Transformation_scheme_id(), ecorePackage.getEString(), "transformation_scheme_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_SCHEME_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(transformationEClass, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, "TRANSFORMATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTRANSFORMATION_Transformation_id(), ecorePackage.getEString(), "transformation_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_Expression(), ecorePackage.getEString(), "expression", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTRANSFORMATION_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html"
		   });
	}

} //Vtl_transformationPackageImpl
