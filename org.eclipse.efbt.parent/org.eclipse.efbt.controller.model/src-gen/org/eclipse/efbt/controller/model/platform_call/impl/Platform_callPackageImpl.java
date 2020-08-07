/**
 */
package org.eclipse.efbt.controller.model.platform_call.impl;

import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.Attribute_lineagePackage;

import org.eclipse.efbt.almengine.model.attribute_lineage.cube_transformation_logic.Cube_transformation_logicPackage;

import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.Row_transformation_logicPackage;

import org.eclipse.efbt.almengine.model.cubes.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.almengine.model.cubes.cubes.CubesPackage;

import org.eclipse.efbt.almengine.model.functions.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.almengine.model.functions.functions.FunctionsPackage;

import org.eclipse.efbt.aorta.aortafunctions.AortafunctionsPackage;

import org.eclipse.efbt.aorta.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.aorta.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.aorta.column_structures.Column_structuresPackage;

import org.eclipse.efbt.aorta.input_data_structures.Input_data_structuresPackage;

import org.eclipse.efbt.aorta.module_management.Module_managementPackage;

import org.eclipse.efbt.aorta.output_data_structures.Output_data_structuresPackage;

import org.eclipse.efbt.aorta.report_structures.Report_structuresPackage;

import org.eclipse.efbt.aorta.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.bird.core.CorePackage;

import org.eclipse.efbt.bird.data_definition.Data_definitionPackage;

import org.eclipse.efbt.bird.rendering.RenderingPackage;

import org.eclipse.efbt.bird.reports.ReportsPackage;

import org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels;
import org.eclipse.efbt.controller.model.platform_call.ExecuteAttributeLineageModel;
import org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell;
import org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.model.platform_call.IsNotSubset;
import org.eclipse.efbt.controller.model.platform_call.IsSubset;
import org.eclipse.efbt.controller.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.controller.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.controller.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.model.platform_call.SubsetBoolean;

import org.eclipse.efbt.trl.model.trl.transformation.TransformationPackage;

import org.eclipse.efbt.trl.model.trl.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.efbt.trl.model.trl.trl_sql_views.Trl_sql_viewsPackage;

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
public class Platform_callPackageImpl extends EPackageImpl implements Platform_callPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass compareAttributeLineageModelsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass isNotSubsetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass isSubsetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subsetBooleanEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass executeAttributeLineageModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass platformCallEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass platformCallModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass getAttributeLineageModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass getAttributeLineageForOneReportCellEClass = null;

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
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Platform_callPackageImpl()
	{
		super(eNS_URI, Platform_callFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Platform_callPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Platform_callPackage init()
	{
		if (isInited) return (Platform_callPackage)EPackage.Registry.INSTANCE.getEPackage(Platform_callPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredPlatform_callPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Platform_callPackageImpl thePlatform_callPackage = registeredPlatform_callPackage instanceof Platform_callPackageImpl ? (Platform_callPackageImpl)registeredPlatform_callPackage : new Platform_callPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		AortafunctionsPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		Column_structuresPackage.eINSTANCE.eClass();
		Input_data_structuresPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Output_data_structuresPackage.eINSTANCE.eClass();
		Report_structuresPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		Attribute_lineagePackage.eINSTANCE.eClass();
		Cube_transformation_logicPackage.eINSTANCE.eClass();
		Row_transformation_logicPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		TransformationPackage.eINSTANCE.eClass();
		Trl_report_cell_viewsPackage.eINSTANCE.eClass();
		Trl_sql_viewsPackage.eINSTANCE.eClass();

		// Create package meta-data objects
		thePlatform_callPackage.createPackageContents();

		// Initialize created meta-data
		thePlatform_callPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		thePlatform_callPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Platform_callPackage.eNS_URI, thePlatform_callPackage);
		return thePlatform_callPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCompareAttributeLineageModels()
	{
		return compareAttributeLineageModelsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCompareAttributeLineageModels_ResultingModel()
	{
		return (EReference)compareAttributeLineageModelsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCompareAttributeLineageModels_SubsetBoolean()
	{
		return (EReference)compareAttributeLineageModelsEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCompareAttributeLineageModels_NotIncludedModel()
	{
		return (EReference)compareAttributeLineageModelsEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCompareAttributeLineageModels_FirstModel()
	{
		return (EReference)compareAttributeLineageModelsEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCompareAttributeLineageModels_SecondModel()
	{
		return (EReference)compareAttributeLineageModelsEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getIsNotSubset()
	{
		return isNotSubsetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getIsSubset()
	{
		return isSubsetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSubsetBoolean()
	{
		return subsetBooleanEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getExecuteAttributeLineageModel()
	{
		return executeAttributeLineageModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExecuteAttributeLineageModel_AttributeLineageModel()
	{
		return (EReference)executeAttributeLineageModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getExecuteAttributeLineageModel_Inputdir()
	{
		return (EAttribute)executeAttributeLineageModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getExecuteAttributeLineageModel_Outputdir()
	{
		return (EAttribute)executeAttributeLineageModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getExecuteAttributeLineageModel_ProgramInputs()
	{
		return (EReference)executeAttributeLineageModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPlatformCall()
	{
		return platformCallEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_ErrorMessage()
	{
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_Name()
	{
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_ReturnStatus()
	{
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPlatformCallModule()
	{
		return platformCallModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getPlatformCallModule_PlatformCalls()
	{
		return (EReference)platformCallModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGetAttributeLineageModel()
	{
		return getAttributeLineageModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageModel_ResultingModel()
	{
		return (EReference)getAttributeLineageModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageModel_TransformationContext()
	{
		return (EReference)getAttributeLineageModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageModel_UnresolvedCubeColumnParams()
	{
		return (EReference)getAttributeLineageModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGetAttributeLineageForOneReportCell()
	{
		return getAttributeLineageForOneReportCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageForOneReportCell_TransformationContext()
	{
		return (EReference)getAttributeLineageForOneReportCellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageForOneReportCell_ReportCell()
	{
		return (EReference)getAttributeLineageForOneReportCellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGetAttributeLineageForOneReportCell_ResultingModel()
	{
		return (EReference)getAttributeLineageForOneReportCellEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Platform_callFactory getPlatform_callFactory()
	{
		return (Platform_callFactory)getEFactoryInstance();
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
		compareAttributeLineageModelsEClass = createEClass(COMPARE_ATTRIBUTE_LINEAGE_MODELS);
		createEReference(compareAttributeLineageModelsEClass, COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL);
		createEReference(compareAttributeLineageModelsEClass, COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN);
		createEReference(compareAttributeLineageModelsEClass, COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL);
		createEReference(compareAttributeLineageModelsEClass, COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL);
		createEReference(compareAttributeLineageModelsEClass, COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL);

		isNotSubsetEClass = createEClass(IS_NOT_SUBSET);

		isSubsetEClass = createEClass(IS_SUBSET);

		subsetBooleanEClass = createEClass(SUBSET_BOOLEAN);

		executeAttributeLineageModelEClass = createEClass(EXECUTE_ATTRIBUTE_LINEAGE_MODEL);
		createEReference(executeAttributeLineageModelEClass, EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL);
		createEAttribute(executeAttributeLineageModelEClass, EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR);
		createEAttribute(executeAttributeLineageModelEClass, EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR);
		createEReference(executeAttributeLineageModelEClass, EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS);

		platformCallEClass = createEClass(PLATFORM_CALL);
		createEAttribute(platformCallEClass, PLATFORM_CALL__ERROR_MESSAGE);
		createEAttribute(platformCallEClass, PLATFORM_CALL__NAME);
		createEAttribute(platformCallEClass, PLATFORM_CALL__RETURN_STATUS);

		platformCallModuleEClass = createEClass(PLATFORM_CALL_MODULE);
		createEReference(platformCallModuleEClass, PLATFORM_CALL_MODULE__PLATFORM_CALLS);

		getAttributeLineageModelEClass = createEClass(GET_ATTRIBUTE_LINEAGE_MODEL);
		createEReference(getAttributeLineageModelEClass, GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL);
		createEReference(getAttributeLineageModelEClass, GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT);
		createEReference(getAttributeLineageModelEClass, GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS);

		getAttributeLineageForOneReportCellEClass = createEClass(GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL);
		createEReference(getAttributeLineageForOneReportCellEClass, GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__TRANSFORMATION_CONTEXT);
		createEReference(getAttributeLineageForOneReportCellEClass, GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__REPORT_CELL);
		createEReference(getAttributeLineageForOneReportCellEClass, GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__RESULTING_MODEL);
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
		Attribute_lineagePackage theAttribute_lineagePackage = (Attribute_lineagePackage)EPackage.Registry.INSTANCE.getEPackage(Attribute_lineagePackage.eNS_URI);
		Base_column_structured_dataPackage theBase_column_structured_dataPackage = (Base_column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Base_column_structured_dataPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		TransformationPackage theTransformationPackage = (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		ReportsPackage theReportsPackage = (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		compareAttributeLineageModelsEClass.getESuperTypes().add(this.getPlatformCall());
		isNotSubsetEClass.getESuperTypes().add(this.getSubsetBoolean());
		isSubsetEClass.getESuperTypes().add(this.getSubsetBoolean());
		executeAttributeLineageModelEClass.getESuperTypes().add(this.getPlatformCall());
		platformCallModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		getAttributeLineageModelEClass.getESuperTypes().add(this.getPlatformCall());
		getAttributeLineageForOneReportCellEClass.getESuperTypes().add(this.getPlatformCall());

		// Initialize classes, features, and operations; add parameters
		initEClass(compareAttributeLineageModelsEClass, CompareAttributeLineageModels.class, "CompareAttributeLineageModels", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCompareAttributeLineageModels_ResultingModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "resultingModel", null, 0, 1, CompareAttributeLineageModels.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCompareAttributeLineageModels_SubsetBoolean(), this.getSubsetBoolean(), null, "subsetBoolean", null, 0, 1, CompareAttributeLineageModels.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCompareAttributeLineageModels_NotIncludedModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "notIncludedModel", null, 0, 1, CompareAttributeLineageModels.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCompareAttributeLineageModels_FirstModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "firstModel", null, 0, 1, CompareAttributeLineageModels.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCompareAttributeLineageModels_SecondModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "secondModel", null, 0, 1, CompareAttributeLineageModels.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(isNotSubsetEClass, IsNotSubset.class, "IsNotSubset", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(isSubsetEClass, IsSubset.class, "IsSubset", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(subsetBooleanEClass, SubsetBoolean.class, "SubsetBoolean", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(executeAttributeLineageModelEClass, ExecuteAttributeLineageModel.class, "ExecuteAttributeLineageModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExecuteAttributeLineageModel_AttributeLineageModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "attributeLineageModel", null, 0, 1, ExecuteAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getExecuteAttributeLineageModel_Inputdir(), ecorePackage.getEString(), "inputdir", null, 0, 1, ExecuteAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getExecuteAttributeLineageModel_Outputdir(), ecorePackage.getEString(), "outputdir", null, 0, 1, ExecuteAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getExecuteAttributeLineageModel_ProgramInputs(), theBase_column_structured_dataPackage.getInputData(), null, "programInputs", null, 0, 1, ExecuteAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(platformCallEClass, PlatformCall.class, "PlatformCall", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getPlatformCall_ErrorMessage(), ecorePackage.getEString(), "errorMessage", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_Name(), ecorePackage.getEString(), "name", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_ReturnStatus(), ecorePackage.getEInt(), "returnStatus", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(platformCallModuleEClass, PlatformCallModule.class, "PlatformCallModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getPlatformCallModule_PlatformCalls(), this.getPlatformCall(), null, "platformCalls", null, 0, -1, PlatformCallModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(getAttributeLineageModelEClass, GetAttributeLineageModel.class, "GetAttributeLineageModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGetAttributeLineageModel_ResultingModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "resultingModel", null, 0, 1, GetAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetAttributeLineageModel_TransformationContext(), theTransformationPackage.getVersionedComponentsSet(), null, "transformationContext", null, 0, 1, GetAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetAttributeLineageModel_UnresolvedCubeColumnParams(), theFunctionsPackage.getSpeculativeCubeColumnParameter(), null, "unresolvedCubeColumnParams", null, 0, -1, GetAttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(getAttributeLineageForOneReportCellEClass, GetAttributeLineageForOneReportCell.class, "GetAttributeLineageForOneReportCell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGetAttributeLineageForOneReportCell_TransformationContext(), theTransformationPackage.getVersionedComponentsSet(), null, "transformationContext", null, 0, 1, GetAttributeLineageForOneReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetAttributeLineageForOneReportCell_ReportCell(), theReportsPackage.getReportCell(), null, "reportCell", null, 0, 1, GetAttributeLineageForOneReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetAttributeLineageForOneReportCell_ResultingModel(), theAttribute_lineagePackage.getAttributeLineageModel(), null, "resultingModel", null, 0, 1, GetAttributeLineageForOneReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (compareAttributeLineageModelsEClass,
		   source,
		   new String[]
		   {
			   "name", "CompareAttributeLineageModels",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCompareAttributeLineageModels_ResultingModel(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "resultingModel"
		   });
		addAnnotation
		  (getCompareAttributeLineageModels_SubsetBoolean(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "subsetBoolean"
		   });
		addAnnotation
		  (getCompareAttributeLineageModels_NotIncludedModel(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "notIncludedModel"
		   });
		addAnnotation
		  (getCompareAttributeLineageModels_FirstModel(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "firstModel"
		   });
		addAnnotation
		  (getCompareAttributeLineageModels_SecondModel(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "secondModel"
		   });
		addAnnotation
		  (isNotSubsetEClass,
		   source,
		   new String[]
		   {
			   "name", "IsNotSubset",
			   "kind", "empty"
		   });
		addAnnotation
		  (isSubsetEClass,
		   source,
		   new String[]
		   {
			   "name", "IsSubset",
			   "kind", "empty"
		   });
		addAnnotation
		  (subsetBooleanEClass,
		   source,
		   new String[]
		   {
			   "name", "SubsetBoolean",
			   "kind", "empty"
		   });
		addAnnotation
		  (executeAttributeLineageModelEClass,
		   source,
		   new String[]
		   {
			   "name", "ExecuteAttributeLineageModel",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getExecuteAttributeLineageModel_AttributeLineageModel(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "attributeLineageModel"
		   });
		addAnnotation
		  (getExecuteAttributeLineageModel_Inputdir(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "inputdir"
		   });
		addAnnotation
		  (getExecuteAttributeLineageModel_Outputdir(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "outputdir"
		   });
		addAnnotation
		  (getExecuteAttributeLineageModel_ProgramInputs(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "programInputs"
		   });
		addAnnotation
		  (platformCallEClass,
		   source,
		   new String[]
		   {
			   "name", "PlatformCall",
			   "kind", "empty"
		   });
		addAnnotation
		  (getPlatformCall_ErrorMessage(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "errorMessage"
		   });
		addAnnotation
		  (getPlatformCall_Name(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getPlatformCall_ReturnStatus(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "returnStatus"
		   });
		addAnnotation
		  (platformCallModuleEClass,
		   source,
		   new String[]
		   {
			   "name", "PlatformCallModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getPlatformCallModule_PlatformCalls(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "platformCalls"
		   });
		addAnnotation
		  (getAttributeLineageModelEClass,
		   source,
		   new String[]
		   {
			   "name", "GetAttributeLineageModel",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getGetAttributeLineageModel_ResultingModel(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "resultingModel"
		   });
		addAnnotation
		  (getGetAttributeLineageModel_TransformationContext(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "transformationContext"
		   });
		addAnnotation
		  (getGetAttributeLineageModel_UnresolvedCubeColumnParams(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "unresolvedCubeColumnParams"
		   });
	}

} //Platform_callPackageImpl
