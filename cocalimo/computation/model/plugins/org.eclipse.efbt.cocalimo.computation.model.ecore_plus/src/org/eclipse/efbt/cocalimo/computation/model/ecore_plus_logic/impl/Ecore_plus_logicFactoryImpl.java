/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_logicFactoryImpl extends EFactoryImpl implements Ecore_plus_logicFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Ecore_plus_logicFactory init() {
		try {
			Ecore_plus_logicFactory theEcore_plus_logicFactory = (Ecore_plus_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Ecore_plus_logicPackage.eNS_URI);
			if (theEcore_plus_logicFactory != null) {
				return theEcore_plus_logicFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Ecore_plus_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY: return createDerivedEntity();
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITY: return createIntermediateDerivedEntity();
			case Ecore_plus_logicPackage.GENERATED_ENTITY: return createGeneratedEntity();
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION_SPEC: return createAggregateFunctionSpec();
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC: return createBasicFunctionSpec();
			case Ecore_plus_logicPackage.BOOLEAN_OPERATION: return createBooleanOperation();
			case Ecore_plus_logicPackage.FUNCTION_SPEC: return createFunctionSpec();
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE: return createFunctionSpecModule();
			case Ecore_plus_logicPackage.PARAMETER: return createParameter();
			case Ecore_plus_logicPackage.MEMBER_PARAMETER: return createMemberParameter();
			case Ecore_plus_logicPackage.ENTITY_PARAMETER: return createEntityParameter();
			case Ecore_plus_logicPackage.VALUE_PARAMETER: return createValueParameter();
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION: return createBasicScenarioOperation();
			case Ecore_plus_logicPackage.FEATURE_PARAMETER: return createFeatureParameter();
			case Ecore_plus_logicPackage.OPERATION_PARAMETER: return createOperationParameter();
			case Ecore_plus_logicPackage.PROXY_OPERATION_PARAMETER: return createProxyOperationParameter();
			case Ecore_plus_logicPackage.PROXY_FEATURE_PARAMETER: return createProxyFeatureParameter();
			case Ecore_plus_logicPackage.CUSTOM_ROW_CREATION_APPROACH: return createCustomRowCreationApproach();
			case Ecore_plus_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: return createFilterAndGroupToOneRowCreationApproach();
			case Ecore_plus_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: return createOneToOneRowCreationApproach();
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH: return createFilterRowCreationApproach();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH: return createRowCreationApproach();
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: return createGroupByRowCreationApproach();
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION: return createRowJoinFunction();
			case Ecore_plus_logicPackage.UNION_ROW_CREATION_APPROACH: return createUnionRowCreationApproach();
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION: return createExplodeArrayOfStructsRowFunction();
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE: return createDerivedEntityModule();
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE: return createIntermediateDerivedEntitiesModule();
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE: return createGeneratedEntityModule();
			case Ecore_plus_logicPackage.ENTITY_TABLE: return createEntityTable();
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE: return createDerivedEntityTable();
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE: return createBaseEntityTable();
			case Ecore_plus_logicPackage.OPERATION: return createOperation();
			case Ecore_plus_logicPackage.BASIC_OPERATION: return createBasicOperation();
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION: return createAggregateOperation();
			case Ecore_plus_logicPackage.ENTITY_FROM_SOURCE_OPERATION: return createEntityFromSourceOperation();
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_OPERATION: return createAttributeFromEntityOperation();
			case Ecore_plus_logicPackage.CELLS_PARAMETER: return createCellsParameter();
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET: return createBasicOperationScenarioSet();
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE: return createBaseEntityTableModule();
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE: return createDerivedEntityTableModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntity createDerivedEntity() {
		DerivedEntityImpl derivedEntity = new DerivedEntityImpl();
		return derivedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public IntermediateDerivedEntity createIntermediateDerivedEntity() {
		IntermediateDerivedEntityImpl intermediateDerivedEntity = new IntermediateDerivedEntityImpl();
		return intermediateDerivedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity createGeneratedEntity() {
		GeneratedEntityImpl generatedEntity = new GeneratedEntityImpl();
		return generatedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateFunctionSpec createAggregateFunctionSpec() {
		AggregateFunctionSpecImpl aggregateFunctionSpec = new AggregateFunctionSpecImpl();
		return aggregateFunctionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunctionSpec createBasicFunctionSpec() {
		BasicFunctionSpecImpl basicFunctionSpec = new BasicFunctionSpecImpl();
		return basicFunctionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BooleanOperation createBooleanOperation() {
		BooleanOperationImpl booleanOperation = new BooleanOperationImpl();
		return booleanOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionSpec createFunctionSpec() {
		FunctionSpecImpl functionSpec = new FunctionSpecImpl();
		return functionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionSpecModule createFunctionSpecModule() {
		FunctionSpecModuleImpl functionSpecModule = new FunctionSpecModuleImpl();
		return functionSpecModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Parameter createParameter() {
		ParameterImpl parameter = new ParameterImpl();
		return parameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberParameter createMemberParameter() {
		MemberParameterImpl memberParameter = new MemberParameterImpl();
		return memberParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityParameter createEntityParameter() {
		EntityParameterImpl entityParameter = new EntityParameterImpl();
		return entityParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ValueParameter createValueParameter() {
		ValueParameterImpl valueParameter = new ValueParameterImpl();
		return valueParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicScenarioOperation createBasicScenarioOperation() {
		BasicScenarioOperationImpl basicScenarioOperation = new BasicScenarioOperationImpl();
		return basicScenarioOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FeatureParameter createFeatureParameter() {
		FeatureParameterImpl featureParameter = new FeatureParameterImpl();
		return featureParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OperationParameter createOperationParameter() {
		OperationParameterImpl operationParameter = new OperationParameterImpl();
		return operationParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ProxyOperationParameter createProxyOperationParameter() {
		ProxyOperationParameterImpl proxyOperationParameter = new ProxyOperationParameterImpl();
		return proxyOperationParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ProxyFeatureParameter createProxyFeatureParameter() {
		ProxyFeatureParameterImpl proxyFeatureParameter = new ProxyFeatureParameterImpl();
		return proxyFeatureParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CustomRowCreationApproach createCustomRowCreationApproach() {
		CustomRowCreationApproachImpl customRowCreationApproach = new CustomRowCreationApproachImpl();
		return customRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FilterAndGroupToOneRowCreationApproach createFilterAndGroupToOneRowCreationApproach() {
		FilterAndGroupToOneRowCreationApproachImpl filterAndGroupToOneRowCreationApproach = new FilterAndGroupToOneRowCreationApproachImpl();
		return filterAndGroupToOneRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OneToOneRowCreationApproach createOneToOneRowCreationApproach() {
		OneToOneRowCreationApproachImpl oneToOneRowCreationApproach = new OneToOneRowCreationApproachImpl();
		return oneToOneRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FilterRowCreationApproach createFilterRowCreationApproach() {
		FilterRowCreationApproachImpl filterRowCreationApproach = new FilterRowCreationApproachImpl();
		return filterRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowCreationApproach createRowCreationApproach() {
		RowCreationApproachImpl rowCreationApproach = new RowCreationApproachImpl();
		return rowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GroupByRowCreationApproach createGroupByRowCreationApproach() {
		GroupByRowCreationApproachImpl groupByRowCreationApproach = new GroupByRowCreationApproachImpl();
		return groupByRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowJoinFunction createRowJoinFunction() {
		RowJoinFunctionImpl rowJoinFunction = new RowJoinFunctionImpl();
		return rowJoinFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnionRowCreationApproach createUnionRowCreationApproach() {
		UnionRowCreationApproachImpl unionRowCreationApproach = new UnionRowCreationApproachImpl();
		return unionRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExplodeArrayOfStructsRowFunction createExplodeArrayOfStructsRowFunction() {
		ExplodeArrayOfStructsRowFunctionImpl explodeArrayOfStructsRowFunction = new ExplodeArrayOfStructsRowFunctionImpl();
		return explodeArrayOfStructsRowFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntityModule createDerivedEntityModule() {
		DerivedEntityModuleImpl derivedEntityModule = new DerivedEntityModuleImpl();
		return derivedEntityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public IntermediateDerivedEntitiesModule createIntermediateDerivedEntitiesModule() {
		IntermediateDerivedEntitiesModuleImpl intermediateDerivedEntitiesModule = new IntermediateDerivedEntitiesModuleImpl();
		return intermediateDerivedEntitiesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntityModule createGeneratedEntityModule() {
		GeneratedEntityModuleImpl generatedEntityModule = new GeneratedEntityModuleImpl();
		return generatedEntityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityTable createEntityTable() {
		EntityTableImpl entityTable = new EntityTableImpl();
		return entityTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntityTable createDerivedEntityTable() {
		DerivedEntityTableImpl derivedEntityTable = new DerivedEntityTableImpl();
		return derivedEntityTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntityTable createBaseEntityTable() {
		BaseEntityTableImpl baseEntityTable = new BaseEntityTableImpl();
		return baseEntityTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Operation createOperation() {
		OperationImpl operation = new OperationImpl();
		return operation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicOperation createBasicOperation() {
		BasicOperationImpl basicOperation = new BasicOperationImpl();
		return basicOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateOperation createAggregateOperation() {
		AggregateOperationImpl aggregateOperation = new AggregateOperationImpl();
		return aggregateOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityFromSourceOperation createEntityFromSourceOperation() {
		EntityFromSourceOperationImpl entityFromSourceOperation = new EntityFromSourceOperationImpl();
		return entityFromSourceOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeFromEntityOperation createAttributeFromEntityOperation() {
		AttributeFromEntityOperationImpl attributeFromEntityOperation = new AttributeFromEntityOperationImpl();
		return attributeFromEntityOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CellsParameter createCellsParameter() {
		CellsParameterImpl cellsParameter = new CellsParameterImpl();
		return cellsParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicOperationScenarioSet createBasicOperationScenarioSet() {
		BasicOperationScenarioSetImpl basicOperationScenarioSet = new BasicOperationScenarioSetImpl();
		return basicOperationScenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntityTableModule createBaseEntityTableModule() {
		BaseEntityTableModuleImpl baseEntityTableModule = new BaseEntityTableModuleImpl();
		return baseEntityTableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntityTableModule createDerivedEntityTableModule() {
		DerivedEntityTableModuleImpl derivedEntityTableModule = new DerivedEntityTableModuleImpl();
		return derivedEntityTableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicPackage getEcore_plus_logicPackage() {
		return (Ecore_plus_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Ecore_plus_logicPackage getPackage() {
		return Ecore_plus_logicPackage.eINSTANCE;
	}

} //Ecore_plus_logicFactoryImpl
