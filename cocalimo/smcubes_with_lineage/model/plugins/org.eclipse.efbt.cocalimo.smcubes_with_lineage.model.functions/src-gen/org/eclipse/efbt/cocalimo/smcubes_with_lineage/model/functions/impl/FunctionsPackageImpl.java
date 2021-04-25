/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunctionSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BooleanFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunctionGroup;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Parameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter;

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
public class FunctionsPackageImpl extends EPackageImpl implements FunctionsPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateFunctionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicFunctionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass booleanFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionSpecModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass paramaterSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass parameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass getStructColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass speculativeStructColumnParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnFunctionGroupEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass speculativeCubeColumnParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass valueParameterEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private FunctionsPackageImpl() {
		super(eNS_URI, FunctionsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link FunctionsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static FunctionsPackage init() {
		if (isInited) return (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredFunctionsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		FunctionsPackageImpl theFunctionsPackage = registeredFunctionsPackage instanceof FunctionsPackageImpl ? (FunctionsPackageImpl)registeredFunctionsPackage : new FunctionsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();

		// Create package meta-data objects
		theFunctionsPackage.createPackageContents();

		// Initialize created meta-data
		theFunctionsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theFunctionsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(FunctionsPackage.eNS_URI, theFunctionsPackage);
		return theFunctionsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateFunction() {
		return aggregateFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateFunction_FunctionSpec() {
		return (EReference)aggregateFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateFunctionSpec() {
		return aggregateFunctionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicFunction() {
		return basicFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicFunction_FunctionSpec() {
		return (EReference)basicFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicFunctionSpec() {
		return basicFunctionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getBasicFunctionSpec_IsInfix() {
		return (EAttribute)basicFunctionSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBooleanFunction() {
		return booleanFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunction() {
		return functionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunction_Parameters() {
		return (EReference)functionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunction_Name() {
		return (EAttribute)functionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunctionSpec() {
		return functionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunctionSpec_ParameterSpecs() {
		return (EReference)functionSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_FunctionalName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_Name() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_RelationalName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunctionSpecModule() {
		return functionSpecModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunctionSpecModule_FunctionSpecs() {
		return (EReference)functionSpecModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getParamaterSpec() {
		return paramaterSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getParamaterSpec_Name() {
		return (EAttribute)paramaterSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getParameter() {
		return parameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGetStructColumnFunction() {
		return getStructColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetStructColumnFunction_ColumnInStruct() {
		return (EReference)getStructColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetStructColumnFunction_CubeColumn() {
		return (EReference)getStructColumnFunctionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSpeculativeStructColumnParameter() {
		return speculativeStructColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_ColumnInsideStruct() {
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_Cube() {
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeStructColumnParameter_StructColumn() {
		return (EReference)speculativeStructColumnParameterEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateColumnFunction() {
		return aggregateColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateColumnFunction_AggregateFunction() {
		return (EReference)aggregateColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicColumnFunction() {
		return basicColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicColumnFunction_BasicFunction() {
		return (EReference)basicColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getColumnFunction() {
		return columnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getColumnFunctionGroup() {
		return columnFunctionGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getColumnFunctionGroup_ColumnFunctions() {
		return (EReference)columnFunctionGroupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCubeColumn() {
		return cubeColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getCubeColumn_Name() {
		return (EAttribute)cubeColumnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCubeColumn_Cube() {
		return (EReference)cubeColumnEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getCubeColumn_UsedInSubsets() {
		return (EAttribute)cubeColumnEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCubeColumn_Variable() {
		return (EReference)cubeColumnEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSpeculativeCubeColumnParameter() {
		return speculativeCubeColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeCubeColumnParameter_Column() {
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeCubeColumnParameter_Cube() {
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getMemberParameter() {
		return memberParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getMemberParameter_Param() {
		return (EReference)memberParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getValueParameter() {
		return valueParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getValueParameter_DataType() {
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getValueParameter_Value() {
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionsFactory getFunctionsFactory() {
		return (FunctionsFactory)getEFactoryInstance();
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
		aggregateFunctionEClass = createEClass(AGGREGATE_FUNCTION);
		createEReference(aggregateFunctionEClass, AGGREGATE_FUNCTION__FUNCTION_SPEC);

		aggregateFunctionSpecEClass = createEClass(AGGREGATE_FUNCTION_SPEC);

		basicFunctionEClass = createEClass(BASIC_FUNCTION);
		createEReference(basicFunctionEClass, BASIC_FUNCTION__FUNCTION_SPEC);

		basicFunctionSpecEClass = createEClass(BASIC_FUNCTION_SPEC);
		createEAttribute(basicFunctionSpecEClass, BASIC_FUNCTION_SPEC__IS_INFIX);

		booleanFunctionEClass = createEClass(BOOLEAN_FUNCTION);

		functionEClass = createEClass(FUNCTION);
		createEReference(functionEClass, FUNCTION__PARAMETERS);
		createEAttribute(functionEClass, FUNCTION__NAME);

		functionSpecEClass = createEClass(FUNCTION_SPEC);
		createEReference(functionSpecEClass, FUNCTION_SPEC__PARAMETER_SPECS);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__FUNCTIONAL_NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__RELATIONAL_NAME);

		functionSpecModuleEClass = createEClass(FUNCTION_SPEC_MODULE);
		createEReference(functionSpecModuleEClass, FUNCTION_SPEC_MODULE__FUNCTION_SPECS);

		paramaterSpecEClass = createEClass(PARAMATER_SPEC);
		createEAttribute(paramaterSpecEClass, PARAMATER_SPEC__NAME);

		parameterEClass = createEClass(PARAMETER);

		getStructColumnFunctionEClass = createEClass(GET_STRUCT_COLUMN_FUNCTION);
		createEReference(getStructColumnFunctionEClass, GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT);
		createEReference(getStructColumnFunctionEClass, GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN);

		speculativeStructColumnParameterEClass = createEClass(SPECULATIVE_STRUCT_COLUMN_PARAMETER);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE);
		createEReference(speculativeStructColumnParameterEClass, SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN);

		aggregateColumnFunctionEClass = createEClass(AGGREGATE_COLUMN_FUNCTION);
		createEReference(aggregateColumnFunctionEClass, AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION);

		basicColumnFunctionEClass = createEClass(BASIC_COLUMN_FUNCTION);
		createEReference(basicColumnFunctionEClass, BASIC_COLUMN_FUNCTION__BASIC_FUNCTION);

		columnFunctionEClass = createEClass(COLUMN_FUNCTION);

		columnFunctionGroupEClass = createEClass(COLUMN_FUNCTION_GROUP);
		createEReference(columnFunctionGroupEClass, COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS);

		cubeColumnEClass = createEClass(CUBE_COLUMN);
		createEAttribute(cubeColumnEClass, CUBE_COLUMN__NAME);
		createEReference(cubeColumnEClass, CUBE_COLUMN__CUBE);
		createEAttribute(cubeColumnEClass, CUBE_COLUMN__USED_IN_SUBSETS);
		createEReference(cubeColumnEClass, CUBE_COLUMN__VARIABLE);

		speculativeCubeColumnParameterEClass = createEClass(SPECULATIVE_CUBE_COLUMN_PARAMETER);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE);

		memberParameterEClass = createEClass(MEMBER_PARAMETER);
		createEReference(memberParameterEClass, MEMBER_PARAMETER__PARAM);

		valueParameterEClass = createEClass(VALUE_PARAMETER);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__DATA_TYPE);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__VALUE);
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
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);
		Efbt_advanced_data_definitionPackage theEfbt_advanced_data_definitionPackage = (Efbt_advanced_data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Efbt_advanced_data_definitionPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		aggregateFunctionEClass.getESuperTypes().add(this.getFunction());
		aggregateFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		basicFunctionEClass.getESuperTypes().add(this.getFunction());
		basicFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		booleanFunctionEClass.getESuperTypes().add(this.getBasicFunction());
		functionEClass.getESuperTypes().add(this.getParameter());
		functionSpecModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		getStructColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		speculativeStructColumnParameterEClass.getESuperTypes().add(this.getParameter());
		aggregateColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		basicColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		columnFunctionEClass.getESuperTypes().add(this.getCubeColumn());
		speculativeCubeColumnParameterEClass.getESuperTypes().add(this.getParameter());
		memberParameterEClass.getESuperTypes().add(this.getParameter());
		valueParameterEClass.getESuperTypes().add(this.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(aggregateFunctionEClass, AggregateFunction.class, "AggregateFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateFunction_FunctionSpec(), this.getAggregateFunctionSpec(), null, "functionSpec", null, 0, 1, AggregateFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateFunctionSpecEClass, AggregateFunctionSpec.class, "AggregateFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(basicFunctionEClass, BasicFunction.class, "BasicFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicFunction_FunctionSpec(), this.getBasicFunctionSpec(), null, "functionSpec", null, 0, 1, BasicFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicFunctionSpecEClass, BasicFunctionSpec.class, "BasicFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBasicFunctionSpec_IsInfix(), ecorePackage.getEBoolean(), "isInfix", null, 0, 1, BasicFunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(booleanFunctionEClass, BooleanFunction.class, "BooleanFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(functionEClass, Function.class, "Function", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunction_Parameters(), this.getParameter(), null, "parameters", null, 0, -1, Function.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunction_Name(), ecorePackage.getEString(), "name", null, 0, 1, Function.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionSpecEClass, FunctionSpec.class, "FunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionSpec_ParameterSpecs(), this.getParamaterSpec(), null, "parameterSpecs", null, 0, -1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_FunctionalName(), ecorePackage.getEString(), "functionalName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_Name(), ecorePackage.getEString(), "name", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_RelationalName(), ecorePackage.getEString(), "relationalName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionSpecModuleEClass, FunctionSpecModule.class, "FunctionSpecModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionSpecModule_FunctionSpecs(), this.getFunctionSpec(), null, "functionSpecs", null, 0, -1, FunctionSpecModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(paramaterSpecEClass, ParamaterSpec.class, "ParamaterSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getParamaterSpec_Name(), ecorePackage.getEString(), "name", null, 0, 1, ParamaterSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(parameterEClass, Parameter.class, "Parameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(getStructColumnFunctionEClass, GetStructColumnFunction.class, "GetStructColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGetStructColumnFunction_ColumnInStruct(), theCorePackage.getVARIABLE(), null, "columnInStruct", null, 0, 1, GetStructColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetStructColumnFunction_CubeColumn(), this.getCubeColumn(), null, "cubeColumn", null, 0, 1, GetStructColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(speculativeStructColumnParameterEClass, SpeculativeStructColumnParameter.class, "SpeculativeStructColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSpeculativeStructColumnParameter_ColumnInsideStruct(), theCorePackage.getVARIABLE(), null, "columnInsideStruct", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeStructColumnParameter_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeStructColumnParameter_StructColumn(), theEfbt_advanced_data_definitionPackage.getStructTypedVariable(), null, "structColumn", null, 0, 1, SpeculativeStructColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateColumnFunctionEClass, AggregateColumnFunction.class, "AggregateColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateColumnFunction_AggregateFunction(), this.getAggregateFunction(), null, "aggregateFunction", null, 0, 1, AggregateColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicColumnFunctionEClass, BasicColumnFunction.class, "BasicColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicColumnFunction_BasicFunction(), this.getBasicFunction(), null, "basicFunction", null, 0, 1, BasicColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnFunctionEClass, ColumnFunction.class, "ColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(columnFunctionGroupEClass, ColumnFunctionGroup.class, "ColumnFunctionGroup", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumnFunctionGroup_ColumnFunctions(), this.getColumnFunction(), null, "columnFunctions", null, 0, -1, ColumnFunctionGroup.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeColumnEClass, CubeColumn.class, "CubeColumn", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCubeColumn_Name(), ecorePackage.getEString(), "name", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeColumn_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCubeColumn_UsedInSubsets(), ecorePackage.getEBoolean(), "usedInSubsets", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeColumn_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(speculativeCubeColumnParameterEClass, SpeculativeCubeColumnParameter.class, "SpeculativeCubeColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSpeculativeCubeColumnParameter_Column(), theCorePackage.getVARIABLE(), null, "column", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeCubeColumnParameter_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberParameterEClass, MemberParameter.class, "MemberParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberParameter_Param(), theCorePackage.getMEMBER(), null, "param", null, 0, 1, MemberParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(valueParameterEClass, ValueParameter.class, "ValueParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getValueParameter_DataType(), theCorePackage.getFACET_VALUE_TYPE(), "dataType", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getValueParameter_Value(), ecorePackage.getEString(), "value", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (aggregateFunctionEClass,
		   source,
		   new String[] {
			   "name", "AggregateFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateFunction_FunctionSpec(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionSpec"
		   });
		addAnnotation
		  (aggregateFunctionSpecEClass,
		   source,
		   new String[] {
			   "name", "AggregateFunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (basicFunctionEClass,
		   source,
		   new String[] {
			   "name", "BasicFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getBasicFunction_FunctionSpec(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionSpec"
		   });
		addAnnotation
		  (basicFunctionSpecEClass,
		   source,
		   new String[] {
			   "name", "BasicFunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getBasicFunctionSpec_IsInfix(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "isInfix"
		   });
		addAnnotation
		  (booleanFunctionEClass,
		   source,
		   new String[] {
			   "name", "BooleanFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (functionEClass,
		   source,
		   new String[] {
			   "name", "Function",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunction_Parameters(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "parameters"
		   });
		addAnnotation
		  (getFunction_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (functionSpecEClass,
		   source,
		   new String[] {
			   "name", "FunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionSpec_ParameterSpecs(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "parameterSpecs"
		   });
		addAnnotation
		  (getFunctionSpec_FunctionalName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionalName"
		   });
		addAnnotation
		  (getFunctionSpec_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getFunctionSpec_RelationalName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "relationalName"
		   });
		addAnnotation
		  (functionSpecModuleEClass,
		   source,
		   new String[] {
			   "name", "FunctionSpecModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionSpecModule_FunctionSpecs(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "functionSpecs"
		   });
		addAnnotation
		  (paramaterSpecEClass,
		   source,
		   new String[] {
			   "name", "ParamaterSpec",
			   "kind", "empty"
		   });
		addAnnotation
		  (getParamaterSpec_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (parameterEClass,
		   source,
		   new String[] {
			   "name", "Parameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (aggregateColumnFunctionEClass,
		   source,
		   new String[] {
			   "name", "AggregateColumnFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateColumnFunction_AggregateFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "aggregateFunction"
		   });
		addAnnotation
		  (basicColumnFunctionEClass,
		   source,
		   new String[] {
			   "name", "BasicColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (getBasicColumnFunction_BasicFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "basicFunction"
		   });
		addAnnotation
		  (columnFunctionEClass,
		   source,
		   new String[] {
			   "name", "ColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (cubeColumnEClass,
		   source,
		   new String[] {
			   "name", "CubeColumn",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCubeColumn_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCubeColumn_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getCubeColumn_UsedInSubsets(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "usedInSubsets"
		   });
		addAnnotation
		  (getCubeColumn_Variable(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable"
		   });
		addAnnotation
		  (speculativeCubeColumnParameterEClass,
		   source,
		   new String[] {
			   "name", "SpeculativeCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSpeculativeCubeColumnParameter_Column(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "column"
		   });
		addAnnotation
		  (getSpeculativeCubeColumnParameter_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (memberParameterEClass,
		   source,
		   new String[] {
			   "name", "MemberParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMemberParameter_Param(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "param"
		   });
		addAnnotation
		  (valueParameterEClass,
		   source,
		   new String[] {
			   "name", "ValueParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getValueParameter_DataType(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "dataType"
		   });
		addAnnotation
		  (getValueParameter_Value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "value"
		   });
	}

} //FunctionsPackageImpl
