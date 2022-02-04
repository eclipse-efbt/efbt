/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CellsParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityFromSourceOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Operation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.Ecore_plus_structurePackageImpl;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_logicPackageImpl extends EPackageImpl implements Ecore_plus_logicPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass intermediateDerivedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntityEClass = null;

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
	private EClass basicFunctionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass booleanOperationEClass = null;

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
	private EClass parameterEClass = null;

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
	private EClass entityParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass valueParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicScenarioOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass featureParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass operationParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass proxyOperationParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass proxyFeatureParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass customRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterAndGroupToOneRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass oneToOneRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass groupByRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowJoinFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unionRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass explodeArrayOfStructsRowFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass intermediateDerivedEntitiesModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass entityTableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityTableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseEntityTableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass operationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass entityFromSourceOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeFromEntityOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellsParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicOperationScenarioSetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseEntityTableModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityTableModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Ecore_plus_logicPackageImpl() {
		super(eNS_URI, Ecore_plus_logicFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Ecore_plus_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Ecore_plus_logicPackage init() {
		if (isInited) return (Ecore_plus_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEcore_plus_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Ecore_plus_logicPackageImpl theEcore_plus_logicPackage = registeredEcore_plus_logicPackage instanceof Ecore_plus_logicPackageImpl ? (Ecore_plus_logicPackageImpl)registeredEcore_plus_logicPackage : new Ecore_plus_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Logical_transformationsPackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_structurePackage.eNS_URI);
		Ecore_plus_structurePackageImpl theEcore_plus_structurePackage = (Ecore_plus_structurePackageImpl)(registeredPackage instanceof Ecore_plus_structurePackageImpl ? registeredPackage : Ecore_plus_structurePackage.eINSTANCE);

		// Create package meta-data objects
		theEcore_plus_logicPackage.createPackageContents();
		theEcore_plus_structurePackage.createPackageContents();

		// Initialize created meta-data
		theEcore_plus_logicPackage.initializePackageContents();
		theEcore_plus_structurePackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEcore_plus_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Ecore_plus_logicPackage.eNS_URI, theEcore_plus_logicPackage);
		return theEcore_plus_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedEntity() {
		return derivedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntity_SourceEntities() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getIntermediateDerivedEntity() {
		return intermediateDerivedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGeneratedEntity() {
		return generatedEntityEClass;
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
	public EClass getBooleanOperation() {
		return booleanOperationEClass;
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
	public EAttribute getFunctionSpec_FunctionalName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_Name() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_SqlName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(2);
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
	public EClass getParameter() {
		return parameterEClass;
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
	public EClass getEntityParameter() {
		return entityParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getEntityParameter_Entity() {
		return (EReference)entityParameterEClass.getEStructuralFeatures().get(0);
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
	public EAttribute getValueParameter_Value() {
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getValueParameter_DataType() {
		return (EReference)valueParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicScenarioOperation() {
		return basicScenarioOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicScenarioOperation_Sceanrio() {
		return (EReference)basicScenarioOperationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicScenarioOperation_FunctionSpec() {
		return (EReference)basicScenarioOperationEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFeatureParameter() {
		return featureParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFeatureParameter_Feature() {
		return (EReference)featureParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOperationParameter() {
		return operationParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOperationParameter_Operation() {
		return (EReference)operationParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getProxyOperationParameter() {
		return proxyOperationParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyOperationParameter_Entity() {
		return (EReference)proxyOperationParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getProxyOperationParameter_Operation_name() {
		return (EAttribute)proxyOperationParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getProxyFeatureParameter() {
		return proxyFeatureParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyFeatureParameter_Entity() {
		return (EReference)proxyFeatureParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getProxyFeatureParameter_Feature_name() {
		return (EAttribute)proxyFeatureParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCustomRowCreationApproach() {
		return customRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFilterAndGroupToOneRowCreationApproach() {
		return filterAndGroupToOneRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterAndGroupToOneRowCreationApproach_FilterFunction() {
		return (EReference)filterAndGroupToOneRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterAndGroupToOneRowCreationApproach_GroupByVariables() {
		return (EReference)filterAndGroupToOneRowCreationApproachEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOneToOneRowCreationApproach() {
		return oneToOneRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFilterRowCreationApproach() {
		return filterRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterRowCreationApproach_FilterFunction() {
		return (EReference)filterRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRowCreationApproach() {
		return rowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproach_CreatedCubeColumns() {
		return (EReference)rowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGroupByRowCreationApproach() {
		return groupByRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGroupByRowCreationApproach_GroupByColumns() {
		return (EReference)groupByRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getGroupByRowCreationApproach_GroupByCubeColumnNames() {
		return (EAttribute)groupByRowCreationApproachEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRowJoinFunction() {
		return rowJoinFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowJoinFunction_JoinFunction() {
		return (EReference)rowJoinFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getUnionRowCreationApproach() {
		return unionRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getExplodeArrayOfStructsRowFunction() {
		return explodeArrayOfStructsRowFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getExplodeArrayOfStructsRowFunction_ArraySourceVariable() {
		return (EReference)explodeArrayOfStructsRowFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedEntityModule() {
		return derivedEntityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntityModule_DerivedEntities() {
		return (EReference)derivedEntityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getIntermediateDerivedEntitiesModule() {
		return intermediateDerivedEntitiesModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getIntermediateDerivedEntitiesModule_IntermediateDerivedEntities() {
		return (EReference)intermediateDerivedEntitiesModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGeneratedEntityModule() {
		return generatedEntityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGeneratedEntityModule_GeneratedEntities() {
		return (EReference)generatedEntityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getEntityTable() {
		return entityTableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedEntityTable() {
		return derivedEntityTableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntityTable_SourceTables() {
		return (EReference)derivedEntityTableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntityTable_ContainedEntityType() {
		return (EReference)derivedEntityTableEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseEntityTable() {
		return baseEntityTableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOperation() {
		return operationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicOperation() {
		return basicOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicOperation_FunctionSpec() {
		return (EReference)basicOperationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateOperation() {
		return aggregateOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateOperation_FunctionSpec() {
		return (EReference)aggregateOperationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getEntityFromSourceOperation() {
		return entityFromSourceOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeFromEntityOperation() {
		return attributeFromEntityOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCellsParameter() {
		return cellsParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicOperationScenarioSet() {
		return basicOperationScenarioSetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicOperationScenarioSet_BasicScenarioLogic() {
		return (EReference)basicOperationScenarioSetEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseEntityTableModule() {
		return baseEntityTableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntityTableModule_BaseEntityTables() {
		return (EReference)baseEntityTableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedEntityTableModule() {
		return derivedEntityTableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntityTableModule_DerivedEntityTables() {
		return (EReference)derivedEntityTableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicFactory getEcore_plus_logicFactory() {
		return (Ecore_plus_logicFactory)getEFactoryInstance();
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
		derivedEntityEClass = createEClass(DERIVED_ENTITY);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__SOURCE_ENTITIES);

		intermediateDerivedEntityEClass = createEClass(INTERMEDIATE_DERIVED_ENTITY);

		generatedEntityEClass = createEClass(GENERATED_ENTITY);

		aggregateFunctionSpecEClass = createEClass(AGGREGATE_FUNCTION_SPEC);

		basicFunctionSpecEClass = createEClass(BASIC_FUNCTION_SPEC);
		createEAttribute(basicFunctionSpecEClass, BASIC_FUNCTION_SPEC__IS_INFIX);

		booleanOperationEClass = createEClass(BOOLEAN_OPERATION);

		functionSpecEClass = createEClass(FUNCTION_SPEC);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__FUNCTIONAL_NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__SQL_NAME);

		functionSpecModuleEClass = createEClass(FUNCTION_SPEC_MODULE);
		createEReference(functionSpecModuleEClass, FUNCTION_SPEC_MODULE__FUNCTION_SPECS);

		parameterEClass = createEClass(PARAMETER);

		memberParameterEClass = createEClass(MEMBER_PARAMETER);
		createEReference(memberParameterEClass, MEMBER_PARAMETER__PARAM);

		entityParameterEClass = createEClass(ENTITY_PARAMETER);
		createEReference(entityParameterEClass, ENTITY_PARAMETER__ENTITY);

		valueParameterEClass = createEClass(VALUE_PARAMETER);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__VALUE);
		createEReference(valueParameterEClass, VALUE_PARAMETER__DATA_TYPE);

		basicScenarioOperationEClass = createEClass(BASIC_SCENARIO_OPERATION);
		createEReference(basicScenarioOperationEClass, BASIC_SCENARIO_OPERATION__SCEANRIO);
		createEReference(basicScenarioOperationEClass, BASIC_SCENARIO_OPERATION__FUNCTION_SPEC);

		featureParameterEClass = createEClass(FEATURE_PARAMETER);
		createEReference(featureParameterEClass, FEATURE_PARAMETER__FEATURE);

		operationParameterEClass = createEClass(OPERATION_PARAMETER);
		createEReference(operationParameterEClass, OPERATION_PARAMETER__OPERATION);

		proxyOperationParameterEClass = createEClass(PROXY_OPERATION_PARAMETER);
		createEReference(proxyOperationParameterEClass, PROXY_OPERATION_PARAMETER__ENTITY);
		createEAttribute(proxyOperationParameterEClass, PROXY_OPERATION_PARAMETER__OPERATION_NAME);

		proxyFeatureParameterEClass = createEClass(PROXY_FEATURE_PARAMETER);
		createEReference(proxyFeatureParameterEClass, PROXY_FEATURE_PARAMETER__ENTITY);
		createEAttribute(proxyFeatureParameterEClass, PROXY_FEATURE_PARAMETER__FEATURE_NAME);

		customRowCreationApproachEClass = createEClass(CUSTOM_ROW_CREATION_APPROACH);

		filterAndGroupToOneRowCreationApproachEClass = createEClass(FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH);
		createEReference(filterAndGroupToOneRowCreationApproachEClass, FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION);
		createEReference(filterAndGroupToOneRowCreationApproachEClass, FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES);

		oneToOneRowCreationApproachEClass = createEClass(ONE_TO_ONE_ROW_CREATION_APPROACH);

		filterRowCreationApproachEClass = createEClass(FILTER_ROW_CREATION_APPROACH);
		createEReference(filterRowCreationApproachEClass, FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION);

		rowCreationApproachEClass = createEClass(ROW_CREATION_APPROACH);
		createEReference(rowCreationApproachEClass, ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS);

		groupByRowCreationApproachEClass = createEClass(GROUP_BY_ROW_CREATION_APPROACH);
		createEReference(groupByRowCreationApproachEClass, GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS);
		createEAttribute(groupByRowCreationApproachEClass, GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES);

		rowJoinFunctionEClass = createEClass(ROW_JOIN_FUNCTION);
		createEReference(rowJoinFunctionEClass, ROW_JOIN_FUNCTION__JOIN_FUNCTION);

		unionRowCreationApproachEClass = createEClass(UNION_ROW_CREATION_APPROACH);

		explodeArrayOfStructsRowFunctionEClass = createEClass(EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION);
		createEReference(explodeArrayOfStructsRowFunctionEClass, EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE);

		derivedEntityModuleEClass = createEClass(DERIVED_ENTITY_MODULE);
		createEReference(derivedEntityModuleEClass, DERIVED_ENTITY_MODULE__DERIVED_ENTITIES);

		intermediateDerivedEntitiesModuleEClass = createEClass(INTERMEDIATE_DERIVED_ENTITIES_MODULE);
		createEReference(intermediateDerivedEntitiesModuleEClass, INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES);

		generatedEntityModuleEClass = createEClass(GENERATED_ENTITY_MODULE);
		createEReference(generatedEntityModuleEClass, GENERATED_ENTITY_MODULE__GENERATED_ENTITIES);

		entityTableEClass = createEClass(ENTITY_TABLE);

		derivedEntityTableEClass = createEClass(DERIVED_ENTITY_TABLE);
		createEReference(derivedEntityTableEClass, DERIVED_ENTITY_TABLE__SOURCE_TABLES);
		createEReference(derivedEntityTableEClass, DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE);

		baseEntityTableEClass = createEClass(BASE_ENTITY_TABLE);

		operationEClass = createEClass(OPERATION);

		basicOperationEClass = createEClass(BASIC_OPERATION);
		createEReference(basicOperationEClass, BASIC_OPERATION__FUNCTION_SPEC);

		aggregateOperationEClass = createEClass(AGGREGATE_OPERATION);
		createEReference(aggregateOperationEClass, AGGREGATE_OPERATION__FUNCTION_SPEC);

		entityFromSourceOperationEClass = createEClass(ENTITY_FROM_SOURCE_OPERATION);

		attributeFromEntityOperationEClass = createEClass(ATTRIBUTE_FROM_ENTITY_OPERATION);

		cellsParameterEClass = createEClass(CELLS_PARAMETER);

		basicOperationScenarioSetEClass = createEClass(BASIC_OPERATION_SCENARIO_SET);
		createEReference(basicOperationScenarioSetEClass, BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC);

		baseEntityTableModuleEClass = createEClass(BASE_ENTITY_TABLE_MODULE);
		createEReference(baseEntityTableModuleEClass, BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES);

		derivedEntityTableModuleEClass = createEClass(DERIVED_ENTITY_TABLE_MODULE);
		createEReference(derivedEntityTableModuleEClass, DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES);
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
		Ecore_plus_structurePackage theEcore_plus_structurePackage = (Ecore_plus_structurePackage)EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_structurePackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);
		Logical_transformationsPackage theLogical_transformationsPackage = (Logical_transformationsPackage)EPackage.Registry.INSTANCE.getEPackage(Logical_transformationsPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		derivedEntityEClass.getESuperTypes().add(theEcore_plus_structurePackage.getEntity());
		intermediateDerivedEntityEClass.getESuperTypes().add(this.getDerivedEntity());
		generatedEntityEClass.getESuperTypes().add(theEcore_plus_structurePackage.getEntity());
		aggregateFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		basicFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		booleanOperationEClass.getESuperTypes().add(this.getBasicOperation());
		functionSpecModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		parameterEClass.getESuperTypes().add(theEcorePackage.getEParameter());
		memberParameterEClass.getESuperTypes().add(this.getParameter());
		entityParameterEClass.getESuperTypes().add(this.getParameter());
		valueParameterEClass.getESuperTypes().add(this.getParameter());
		basicScenarioOperationEClass.getESuperTypes().add(this.getOperation());
		featureParameterEClass.getESuperTypes().add(this.getParameter());
		operationParameterEClass.getESuperTypes().add(this.getParameter());
		proxyOperationParameterEClass.getESuperTypes().add(this.getParameter());
		customRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		filterAndGroupToOneRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		oneToOneRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		filterRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		rowCreationApproachEClass.getESuperTypes().add(theEcorePackage.getEOperation());
		groupByRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		rowJoinFunctionEClass.getESuperTypes().add(this.getRowCreationApproach());
		unionRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		explodeArrayOfStructsRowFunctionEClass.getESuperTypes().add(this.getRowCreationApproach());
		derivedEntityModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		intermediateDerivedEntitiesModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		generatedEntityModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		entityTableEClass.getESuperTypes().add(theEcore_plus_structurePackage.getEntity());
		derivedEntityTableEClass.getESuperTypes().add(this.getEntityTable());
		baseEntityTableEClass.getESuperTypes().add(this.getEntityTable());
		operationEClass.getESuperTypes().add(theEcorePackage.getEOperation());
		operationEClass.getESuperTypes().add(this.getParameter());
		basicOperationEClass.getESuperTypes().add(this.getOperation());
		aggregateOperationEClass.getESuperTypes().add(this.getOperation());
		entityFromSourceOperationEClass.getESuperTypes().add(this.getOperation());
		attributeFromEntityOperationEClass.getESuperTypes().add(this.getOperation());
		cellsParameterEClass.getESuperTypes().add(this.getParameter());
		basicOperationScenarioSetEClass.getESuperTypes().add(this.getOperation());
		baseEntityTableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		derivedEntityTableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(derivedEntityEClass, DerivedEntity.class, "DerivedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntity_SourceEntities(), theEcore_plus_structurePackage.getEntity(), null, "sourceEntities", null, 0, -1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(intermediateDerivedEntityEClass, IntermediateDerivedEntity.class, "IntermediateDerivedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(generatedEntityEClass, GeneratedEntity.class, "GeneratedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(aggregateFunctionSpecEClass, AggregateFunctionSpec.class, "AggregateFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(basicFunctionSpecEClass, BasicFunctionSpec.class, "BasicFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBasicFunctionSpec_IsInfix(), ecorePackage.getEBoolean(), "isInfix", null, 0, 1, BasicFunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(booleanOperationEClass, BooleanOperation.class, "BooleanOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(functionSpecEClass, FunctionSpec.class, "FunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getFunctionSpec_FunctionalName(), ecorePackage.getEString(), "functionalName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_Name(), ecorePackage.getEString(), "name", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_SqlName(), ecorePackage.getEString(), "sqlName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionSpecModuleEClass, FunctionSpecModule.class, "FunctionSpecModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionSpecModule_FunctionSpecs(), this.getFunctionSpec(), null, "functionSpecs", null, 0, -1, FunctionSpecModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(parameterEClass, Parameter.class, "Parameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(memberParameterEClass, MemberParameter.class, "MemberParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberParameter_Param(), theEcorePackage.getEEnumLiteral(), null, "param", null, 0, 1, MemberParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityParameterEClass, EntityParameter.class, "EntityParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getEntityParameter_Entity(), theEcore_plus_structurePackage.getEntity(), null, "entity", null, 0, 1, EntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(valueParameterEClass, ValueParameter.class, "ValueParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getValueParameter_Value(), ecorePackage.getEString(), "value", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getValueParameter_DataType(), theEcorePackage.getEDataType(), null, "dataType", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicScenarioOperationEClass, BasicScenarioOperation.class, "BasicScenarioOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicScenarioOperation_Sceanrio(), theLogical_transformationsPackage.getScenario(), null, "sceanrio", null, 0, 1, BasicScenarioOperation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBasicScenarioOperation_FunctionSpec(), this.getBasicFunctionSpec(), null, "functionSpec", null, 0, 1, BasicScenarioOperation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(featureParameterEClass, FeatureParameter.class, "FeatureParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFeatureParameter_Feature(), theEcorePackage.getEStructuralFeature(), null, "feature", null, 0, 1, FeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(operationParameterEClass, OperationParameter.class, "OperationParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getOperationParameter_Operation(), this.getParameter(), null, "operation", null, 0, 1, OperationParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(proxyOperationParameterEClass, ProxyOperationParameter.class, "ProxyOperationParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getProxyOperationParameter_Entity(), theEcore_plus_structurePackage.getEntity(), null, "entity", null, 0, 1, ProxyOperationParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getProxyOperationParameter_Operation_name(), ecorePackage.getEString(), "operation_name", null, 0, 1, ProxyOperationParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(proxyFeatureParameterEClass, ProxyFeatureParameter.class, "ProxyFeatureParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getProxyFeatureParameter_Entity(), theEcore_plus_structurePackage.getEntity(), null, "entity", null, 0, 1, ProxyFeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getProxyFeatureParameter_Feature_name(), ecorePackage.getEString(), "feature_name", null, 0, 1, ProxyFeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(customRowCreationApproachEClass, CustomRowCreationApproach.class, "CustomRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(filterAndGroupToOneRowCreationApproachEClass, FilterAndGroupToOneRowCreationApproach.class, "FilterAndGroupToOneRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterAndGroupToOneRowCreationApproach_FilterFunction(), this.getBooleanOperation(), null, "filterFunction", null, 0, 1, FilterAndGroupToOneRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFilterAndGroupToOneRowCreationApproach_GroupByVariables(), theEcorePackage.getETypedElement(), null, "groupByVariables", null, 0, -1, FilterAndGroupToOneRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(oneToOneRowCreationApproachEClass, OneToOneRowCreationApproach.class, "OneToOneRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(filterRowCreationApproachEClass, FilterRowCreationApproach.class, "FilterRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterRowCreationApproach_FilterFunction(), this.getBooleanOperation(), null, "filterFunction", null, 0, 1, FilterRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowCreationApproachEClass, RowCreationApproach.class, "RowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRowCreationApproach_CreatedCubeColumns(), theEcorePackage.getEAttribute(), null, "createdCubeColumns", null, 0, -1, RowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(groupByRowCreationApproachEClass, GroupByRowCreationApproach.class, "GroupByRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGroupByRowCreationApproach_GroupByColumns(), theEcorePackage.getETypedElement(), null, "groupByColumns", null, 0, -1, GroupByRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getGroupByRowCreationApproach_GroupByCubeColumnNames(), ecorePackage.getEString(), "groupByCubeColumnNames", null, 0, -1, GroupByRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowJoinFunctionEClass, RowJoinFunction.class, "RowJoinFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRowJoinFunction_JoinFunction(), this.getBooleanOperation(), null, "joinFunction", null, 0, 1, RowJoinFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(unionRowCreationApproachEClass, UnionRowCreationApproach.class, "UnionRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(explodeArrayOfStructsRowFunctionEClass, ExplodeArrayOfStructsRowFunction.class, "ExplodeArrayOfStructsRowFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExplodeArrayOfStructsRowFunction_ArraySourceVariable(), theEcorePackage.getETypedElement(), null, "arraySourceVariable", null, 0, 1, ExplodeArrayOfStructsRowFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedEntityModuleEClass, DerivedEntityModule.class, "DerivedEntityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntityModule_DerivedEntities(), this.getDerivedEntity(), null, "derivedEntities", null, 0, -1, DerivedEntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(intermediateDerivedEntitiesModuleEClass, IntermediateDerivedEntitiesModule.class, "IntermediateDerivedEntitiesModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getIntermediateDerivedEntitiesModule_IntermediateDerivedEntities(), this.getIntermediateDerivedEntity(), null, "intermediateDerivedEntities", null, 0, -1, IntermediateDerivedEntitiesModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(generatedEntityModuleEClass, GeneratedEntityModule.class, "GeneratedEntityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGeneratedEntityModule_GeneratedEntities(), this.getGeneratedEntity(), null, "generatedEntities", null, 0, -1, GeneratedEntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityTableEClass, EntityTable.class, "EntityTable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(derivedEntityTableEClass, DerivedEntityTable.class, "DerivedEntityTable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntityTable_SourceTables(), this.getEntityTable(), null, "sourceTables", null, 0, -1, DerivedEntityTable.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedEntityTable_ContainedEntityType(), this.getDerivedEntity(), null, "containedEntityType", null, 0, 1, DerivedEntityTable.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseEntityTableEClass, BaseEntityTable.class, "BaseEntityTable", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(operationEClass, Operation.class, "Operation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(basicOperationEClass, BasicOperation.class, "BasicOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicOperation_FunctionSpec(), this.getBasicFunctionSpec(), null, "functionSpec", null, 0, 1, BasicOperation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateOperationEClass, AggregateOperation.class, "AggregateOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateOperation_FunctionSpec(), this.getAggregateFunctionSpec(), null, "functionSpec", null, 0, 1, AggregateOperation.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityFromSourceOperationEClass, EntityFromSourceOperation.class, "EntityFromSourceOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(attributeFromEntityOperationEClass, AttributeFromEntityOperation.class, "AttributeFromEntityOperation", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(cellsParameterEClass, CellsParameter.class, "CellsParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(basicOperationScenarioSetEClass, BasicOperationScenarioSet.class, "BasicOperationScenarioSet", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicOperationScenarioSet_BasicScenarioLogic(), this.getBasicScenarioOperation(), null, "basicScenarioLogic", null, 0, -1, BasicOperationScenarioSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseEntityTableModuleEClass, BaseEntityTableModule.class, "BaseEntityTableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseEntityTableModule_BaseEntityTables(), this.getBaseEntityTable(), null, "baseEntityTables", null, 0, -1, BaseEntityTableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedEntityTableModuleEClass, DerivedEntityTableModule.class, "DerivedEntityTableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntityTableModule_DerivedEntityTables(), this.getDerivedEntityTable(), null, "derivedEntityTables", null, 0, -1, DerivedEntityTableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
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
		  (aggregateFunctionSpecEClass,
		   source,
		   new String[] {
			   "name", "AggregateFunctionSpec",
			   "kind", "elementOnly"
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
		  (booleanOperationEClass,
		   source,
		   new String[] {
			   "name", "BooleanFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (functionSpecEClass,
		   source,
		   new String[] {
			   "name", "FunctionSpec",
			   "kind", "elementOnly"
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
		  (getFunctionSpec_SqlName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "sqlName"
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
		  (parameterEClass,
		   source,
		   new String[] {
			   "name", "Parameter",
			   "kind", "empty"
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
		  (getValueParameter_Value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "value"
		   });
		addAnnotation
		  (featureParameterEClass,
		   source,
		   new String[] {
			   "name", "ResolvedCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFeatureParameter_Feature(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cubeColumn"
		   });
		addAnnotation
		  (proxyOperationParameterEClass,
		   source,
		   new String[] {
			   "name", "SpeculativeCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getProxyOperationParameter_Entity(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getProxyFeatureParameter_Entity(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (customRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "CustomRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (filterAndGroupToOneRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "FilterAndGroupToOneRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterAndGroupToOneRowCreationApproach_FilterFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "filterFunction"
		   });
		addAnnotation
		  (oneToOneRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "OneToOneRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (filterRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "FilterRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterRowCreationApproach_FilterFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "filterFunction"
		   });
		addAnnotation
		  (rowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "RowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRowCreationApproach_CreatedCubeColumns(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "createdCubeColumns"
		   });
		addAnnotation
		  (groupByRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "GroupByRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (rowJoinFunctionEClass,
		   source,
		   new String[] {
			   "name", "RowJoinFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRowJoinFunction_JoinFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "joinFunction"
		   });
		addAnnotation
		  (unionRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "UnionRowCreationApproach",
			   "kind", "elementOnly"
		   });
	}

} //Ecore_plus_logicPackageImpl
