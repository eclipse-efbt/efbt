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
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION: return createAggregateFunction();
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION_SPEC: return createAggregateFunctionSpec();
			case Ecore_plus_logicPackage.BASIC_FUNCTION: return createBasicFunction();
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC: return createBasicFunctionSpec();
			case Ecore_plus_logicPackage.BOOLEAN_FUNCTION: return createBooleanFunction();
			case Ecore_plus_logicPackage.FUNCTION: return createFunction();
			case Ecore_plus_logicPackage.FUNCTION_SPEC: return createFunctionSpec();
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE: return createFunctionSpecModule();
			case Ecore_plus_logicPackage.PARAMATER_SPEC: return createParamaterSpec();
			case Ecore_plus_logicPackage.PARAMETER: return createParameter();
			case Ecore_plus_logicPackage.MEMBER_PARAMETER: return createMemberParameter();
			case Ecore_plus_logicPackage.VALUE_PARAMETER: return createValueParameter();
			case Ecore_plus_logicPackage.AGGREGATE_DERIVED_FEATURE: return createAggregateDerivedFeature();
			case Ecore_plus_logicPackage.BASIC_DERIVED_FEATURE: return createBasicDerivedFeature();
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC: return createBasicScenarioLogic();
			case Ecore_plus_logicPackage.DERIVED_FEATURE: return createDerivedFeature();
			case Ecore_plus_logicPackage.BASE_FEATURE: return createBaseFeature();
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP: return createDerivedFeatureGroup();
			case Ecore_plus_logicPackage.FEATURE_PARAMETER: return createFeatureParameter();
			case Ecore_plus_logicPackage.PROXY_FEATURE_PARAMETER: return createProxyFeatureParameter();
			case Ecore_plus_logicPackage.GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE: return createGetAttributeFromEntityDerivedFeature();
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER: return createAttributeFromEntityParameter();
			case Ecore_plus_logicPackage.PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER: return createProxyAttributeFromEntityParameter();
			case Ecore_plus_logicPackage.CUSTOM_ROW_CREATION_APPROACH: return createCustomRowCreationApproach();
			case Ecore_plus_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: return createFilterAndGroupToOneRowCreationApproach();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY: return createRowCreationApproachForEntity();
			case Ecore_plus_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: return createOneToOneRowCreationApproach();
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH: return createFilterRowCreationApproach();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH: return createRowCreationApproach();
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: return createGroupByRowCreationApproach();
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION: return createRowJoinFunction();
			case Ecore_plus_logicPackage.UNION_ROW_CREATION_APPROACH: return createUnionRowCreationApproach();
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION: return createExplodeArrayOfStructsRowFunction();
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
	public AggregateFunction createAggregateFunction() {
		AggregateFunctionImpl aggregateFunction = new AggregateFunctionImpl();
		return aggregateFunction;
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
	public BasicFunction createBasicFunction() {
		BasicFunctionImpl basicFunction = new BasicFunctionImpl();
		return basicFunction;
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
	public BooleanFunction createBooleanFunction() {
		BooleanFunctionImpl booleanFunction = new BooleanFunctionImpl();
		return booleanFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Function createFunction() {
		FunctionImpl function = new FunctionImpl();
		return function;
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
	public ParamaterSpec createParamaterSpec() {
		ParamaterSpecImpl paramaterSpec = new ParamaterSpecImpl();
		return paramaterSpec;
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
	public ValueParameter createValueParameter() {
		ValueParameterImpl valueParameter = new ValueParameterImpl();
		return valueParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateDerivedFeature createAggregateDerivedFeature() {
		AggregateDerivedFeatureImpl aggregateDerivedFeature = new AggregateDerivedFeatureImpl();
		return aggregateDerivedFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicDerivedFeature createBasicDerivedFeature() {
		BasicDerivedFeatureImpl basicDerivedFeature = new BasicDerivedFeatureImpl();
		return basicDerivedFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicScenarioLogic createBasicScenarioLogic() {
		BasicScenarioLogicImpl basicScenarioLogic = new BasicScenarioLogicImpl();
		return basicScenarioLogic;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedFeature createDerivedFeature() {
		DerivedFeatureImpl derivedFeature = new DerivedFeatureImpl();
		return derivedFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseFeature createBaseFeature() {
		BaseFeatureImpl baseFeature = new BaseFeatureImpl();
		return baseFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedFeatureGroup createDerivedFeatureGroup() {
		DerivedFeatureGroupImpl derivedFeatureGroup = new DerivedFeatureGroupImpl();
		return derivedFeatureGroup;
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
	public ProxyFeatureParameter createProxyFeatureParameter() {
		ProxyFeatureParameterImpl proxyFeatureParameter = new ProxyFeatureParameterImpl();
		return proxyFeatureParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GetAttributeFromEntityDerivedFeature createGetAttributeFromEntityDerivedFeature() {
		GetAttributeFromEntityDerivedFeatureImpl getAttributeFromEntityDerivedFeature = new GetAttributeFromEntityDerivedFeatureImpl();
		return getAttributeFromEntityDerivedFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeFromEntityParameter createAttributeFromEntityParameter() {
		AttributeFromEntityParameterImpl attributeFromEntityParameter = new AttributeFromEntityParameterImpl();
		return attributeFromEntityParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ProxyAttributeFromEntityParameter createProxyAttributeFromEntityParameter() {
		ProxyAttributeFromEntityParameterImpl proxyAttributeFromEntityParameter = new ProxyAttributeFromEntityParameterImpl();
		return proxyAttributeFromEntityParameter;
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
	public RowCreationApproachForEntity createRowCreationApproachForEntity() {
		RowCreationApproachForEntityImpl rowCreationApproachForEntity = new RowCreationApproachForEntityImpl();
		return rowCreationApproachForEntity;
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
