/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.util;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.*;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.ETypedElement;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage
 * @generated
 */
public class Ecore_plus_logicSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Ecore_plus_logicPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicSwitch() {
		if (modelPackage == null) {
			modelPackage = Ecore_plus_logicPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY: {
				DerivedEntity derivedEntity = (DerivedEntity)theEObject;
				T result = caseDerivedEntity(derivedEntity);
				if (result == null) result = caseEntity(derivedEntity);
				if (result == null) result = caseEClass(derivedEntity);
				if (result == null) result = caseEClassifier(derivedEntity);
				if (result == null) result = caseENamedElement(derivedEntity);
				if (result == null) result = caseEModelElement(derivedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITY: {
				IntermediateDerivedEntity intermediateDerivedEntity = (IntermediateDerivedEntity)theEObject;
				T result = caseIntermediateDerivedEntity(intermediateDerivedEntity);
				if (result == null) result = caseDerivedEntity(intermediateDerivedEntity);
				if (result == null) result = caseEntity(intermediateDerivedEntity);
				if (result == null) result = caseEClass(intermediateDerivedEntity);
				if (result == null) result = caseEClassifier(intermediateDerivedEntity);
				if (result == null) result = caseENamedElement(intermediateDerivedEntity);
				if (result == null) result = caseEModelElement(intermediateDerivedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.GENERATED_ENTITY: {
				GeneratedEntity generatedEntity = (GeneratedEntity)theEObject;
				T result = caseGeneratedEntity(generatedEntity);
				if (result == null) result = caseEntity(generatedEntity);
				if (result == null) result = caseEClass(generatedEntity);
				if (result == null) result = caseEClassifier(generatedEntity);
				if (result == null) result = caseENamedElement(generatedEntity);
				if (result == null) result = caseEModelElement(generatedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION: {
				AggregateFunction aggregateFunction = (AggregateFunction)theEObject;
				T result = caseAggregateFunction(aggregateFunction);
				if (result == null) result = caseFunction(aggregateFunction);
				if (result == null) result = caseParameter(aggregateFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION_SPEC: {
				AggregateFunctionSpec aggregateFunctionSpec = (AggregateFunctionSpec)theEObject;
				T result = caseAggregateFunctionSpec(aggregateFunctionSpec);
				if (result == null) result = caseFunctionSpec(aggregateFunctionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_FUNCTION: {
				BasicFunction basicFunction = (BasicFunction)theEObject;
				T result = caseBasicFunction(basicFunction);
				if (result == null) result = caseFunction(basicFunction);
				if (result == null) result = caseParameter(basicFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC: {
				BasicFunctionSpec basicFunctionSpec = (BasicFunctionSpec)theEObject;
				T result = caseBasicFunctionSpec(basicFunctionSpec);
				if (result == null) result = caseFunctionSpec(basicFunctionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BOOLEAN_FUNCTION: {
				BooleanFunction booleanFunction = (BooleanFunction)theEObject;
				T result = caseBooleanFunction(booleanFunction);
				if (result == null) result = caseBasicFunction(booleanFunction);
				if (result == null) result = caseFunction(booleanFunction);
				if (result == null) result = caseParameter(booleanFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FUNCTION: {
				Function function = (Function)theEObject;
				T result = caseFunction(function);
				if (result == null) result = caseParameter(function);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FUNCTION_SPEC: {
				FunctionSpec functionSpec = (FunctionSpec)theEObject;
				T result = caseFunctionSpec(functionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE: {
				FunctionSpecModule functionSpecModule = (FunctionSpecModule)theEObject;
				T result = caseFunctionSpecModule(functionSpecModule);
				if (result == null) result = caseModule(functionSpecModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PARAMATER_SPEC: {
				ParamaterSpec paramaterSpec = (ParamaterSpec)theEObject;
				T result = caseParamaterSpec(paramaterSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PARAMETER: {
				Parameter parameter = (Parameter)theEObject;
				T result = caseParameter(parameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.MEMBER_PARAMETER: {
				MemberParameter memberParameter = (MemberParameter)theEObject;
				T result = caseMemberParameter(memberParameter);
				if (result == null) result = caseParameter(memberParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.VALUE_PARAMETER: {
				ValueParameter valueParameter = (ValueParameter)theEObject;
				T result = caseValueParameter(valueParameter);
				if (result == null) result = caseParameter(valueParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.AGGREGATE_DERIVED_FEATURE: {
				AggregateDerivedFeature aggregateDerivedFeature = (AggregateDerivedFeature)theEObject;
				T result = caseAggregateDerivedFeature(aggregateDerivedFeature);
				if (result == null) result = caseDerivedFeature(aggregateDerivedFeature);
				if (result == null) result = caseEStructuralFeature(aggregateDerivedFeature);
				if (result == null) result = caseETypedElement(aggregateDerivedFeature);
				if (result == null) result = caseENamedElement(aggregateDerivedFeature);
				if (result == null) result = caseEModelElement(aggregateDerivedFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_DERIVED_FEATURE: {
				BasicDerivedFeature basicDerivedFeature = (BasicDerivedFeature)theEObject;
				T result = caseBasicDerivedFeature(basicDerivedFeature);
				if (result == null) result = caseDerivedFeature(basicDerivedFeature);
				if (result == null) result = caseEStructuralFeature(basicDerivedFeature);
				if (result == null) result = caseETypedElement(basicDerivedFeature);
				if (result == null) result = caseENamedElement(basicDerivedFeature);
				if (result == null) result = caseEModelElement(basicDerivedFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC: {
				BasicScenarioLogic basicScenarioLogic = (BasicScenarioLogic)theEObject;
				T result = caseBasicScenarioLogic(basicScenarioLogic);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.DERIVED_FEATURE: {
				DerivedFeature derivedFeature = (DerivedFeature)theEObject;
				T result = caseDerivedFeature(derivedFeature);
				if (result == null) result = caseEStructuralFeature(derivedFeature);
				if (result == null) result = caseETypedElement(derivedFeature);
				if (result == null) result = caseENamedElement(derivedFeature);
				if (result == null) result = caseEModelElement(derivedFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASE_FEATURE: {
				BaseFeature baseFeature = (BaseFeature)theEObject;
				T result = caseBaseFeature(baseFeature);
				if (result == null) result = caseEAttribute(baseFeature);
				if (result == null) result = caseEStructuralFeature(baseFeature);
				if (result == null) result = caseETypedElement(baseFeature);
				if (result == null) result = caseENamedElement(baseFeature);
				if (result == null) result = caseEModelElement(baseFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP: {
				DerivedFeatureGroup derivedFeatureGroup = (DerivedFeatureGroup)theEObject;
				T result = caseDerivedFeatureGroup(derivedFeatureGroup);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FEATURE_PARAMETER: {
				FeatureParameter featureParameter = (FeatureParameter)theEObject;
				T result = caseFeatureParameter(featureParameter);
				if (result == null) result = caseParameter(featureParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PROXY_FEATURE_PARAMETER: {
				ProxyFeatureParameter proxyFeatureParameter = (ProxyFeatureParameter)theEObject;
				T result = caseProxyFeatureParameter(proxyFeatureParameter);
				if (result == null) result = caseParameter(proxyFeatureParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE: {
				GetAttributeFromEntityDerivedFeature getAttributeFromEntityDerivedFeature = (GetAttributeFromEntityDerivedFeature)theEObject;
				T result = caseGetAttributeFromEntityDerivedFeature(getAttributeFromEntityDerivedFeature);
				if (result == null) result = caseDerivedFeature(getAttributeFromEntityDerivedFeature);
				if (result == null) result = caseEStructuralFeature(getAttributeFromEntityDerivedFeature);
				if (result == null) result = caseETypedElement(getAttributeFromEntityDerivedFeature);
				if (result == null) result = caseENamedElement(getAttributeFromEntityDerivedFeature);
				if (result == null) result = caseEModelElement(getAttributeFromEntityDerivedFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER: {
				AttributeFromEntityParameter attributeFromEntityParameter = (AttributeFromEntityParameter)theEObject;
				T result = caseAttributeFromEntityParameter(attributeFromEntityParameter);
				if (result == null) result = caseEParameter(attributeFromEntityParameter);
				if (result == null) result = caseETypedElement(attributeFromEntityParameter);
				if (result == null) result = caseENamedElement(attributeFromEntityParameter);
				if (result == null) result = caseEModelElement(attributeFromEntityParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER: {
				ProxyAttributeFromEntityParameter proxyAttributeFromEntityParameter = (ProxyAttributeFromEntityParameter)theEObject;
				T result = caseProxyAttributeFromEntityParameter(proxyAttributeFromEntityParameter);
				if (result == null) result = caseParameter(proxyAttributeFromEntityParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.CUSTOM_ROW_CREATION_APPROACH: {
				CustomRowCreationApproach customRowCreationApproach = (CustomRowCreationApproach)theEObject;
				T result = caseCustomRowCreationApproach(customRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(customRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: {
				FilterAndGroupToOneRowCreationApproach filterAndGroupToOneRowCreationApproach = (FilterAndGroupToOneRowCreationApproach)theEObject;
				T result = caseFilterAndGroupToOneRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY: {
				RowCreationApproachForEntity rowCreationApproachForEntity = (RowCreationApproachForEntity)theEObject;
				T result = caseRowCreationApproachForEntity(rowCreationApproachForEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: {
				OneToOneRowCreationApproach oneToOneRowCreationApproach = (OneToOneRowCreationApproach)theEObject;
				T result = caseOneToOneRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH: {
				FilterRowCreationApproach filterRowCreationApproach = (FilterRowCreationApproach)theEObject;
				T result = caseFilterRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH: {
				RowCreationApproach rowCreationApproach = (RowCreationApproach)theEObject;
				T result = caseRowCreationApproach(rowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: {
				GroupByRowCreationApproach groupByRowCreationApproach = (GroupByRowCreationApproach)theEObject;
				T result = caseGroupByRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION: {
				RowJoinFunction rowJoinFunction = (RowJoinFunction)theEObject;
				T result = caseRowJoinFunction(rowJoinFunction);
				if (result == null) result = caseRowCreationApproach(rowJoinFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.UNION_ROW_CREATION_APPROACH: {
				UnionRowCreationApproach unionRowCreationApproach = (UnionRowCreationApproach)theEObject;
				T result = caseUnionRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION: {
				ExplodeArrayOfStructsRowFunction explodeArrayOfStructsRowFunction = (ExplodeArrayOfStructsRowFunction)theEObject;
				T result = caseExplodeArrayOfStructsRowFunction(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseRowCreationApproach(explodeArrayOfStructsRowFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedEntity(DerivedEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Intermediate Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Intermediate Derived Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseIntermediateDerivedEntity(IntermediateDerivedEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generated Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generated Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGeneratedEntity(GeneratedEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateFunction(AggregateFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Function Spec</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateFunctionSpec(AggregateFunctionSpec object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicFunction(BasicFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Function Spec</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicFunctionSpec(BasicFunctionSpec object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Boolean Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Boolean Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBooleanFunction(BooleanFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunction(Function object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Function Spec</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionSpec(FunctionSpec object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Function Spec Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Function Spec Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionSpecModule(FunctionSpecModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Paramater Spec</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Paramater Spec</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParamaterSpec(ParamaterSpec object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParameter(Parameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberParameter(MemberParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Value Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Value Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseValueParameter(ValueParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Derived Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Derived Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateDerivedFeature(AggregateDerivedFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Derived Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Derived Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicDerivedFeature(BasicDerivedFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Scenario Logic</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Scenario Logic</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicScenarioLogic(BasicScenarioLogic object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedFeature(DerivedFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseFeature(BaseFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Feature Group</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Feature Group</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedFeatureGroup(DerivedFeatureGroup object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Feature Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Feature Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFeatureParameter(FeatureParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Proxy Feature Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Proxy Feature Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseProxyFeatureParameter(ProxyFeatureParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Get Attribute From Entity Derived Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Get Attribute From Entity Derived Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGetAttributeFromEntityDerivedFeature(GetAttributeFromEntityDerivedFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute From Entity Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute From Entity Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttributeFromEntityParameter(AttributeFromEntityParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Proxy Attribute From Entity Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Proxy Attribute From Entity Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseProxyAttributeFromEntityParameter(ProxyAttributeFromEntityParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Custom Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Custom Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCustomRowCreationApproach(CustomRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter And Group To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter And Group To One Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterAndGroupToOneRowCreationApproach(FilterAndGroupToOneRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Creation Approach For Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Creation Approach For Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowCreationApproachForEntity(RowCreationApproachForEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>One To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>One To One Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOneToOneRowCreationApproach(OneToOneRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterRowCreationApproach(FilterRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowCreationApproach(RowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Group By Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Group By Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGroupByRowCreationApproach(GroupByRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Join Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Join Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowJoinFunction(RowJoinFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Union Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Union Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUnionRowCreationApproach(UnionRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Explode Array Of Structs Row Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Explode Array Of Structs Row Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseExplodeArrayOfStructsRowFunction(ExplodeArrayOfStructsRowFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEModelElement(EModelElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseENamedElement(ENamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EClassifier</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EClassifier</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEClassifier(EClassifier object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EClass</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EClass</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEClass(EClass object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntity(Entity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseETypedElement(ETypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEStructuralFeature(EStructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EAttribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EAttribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEAttribute(EAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EParameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EParameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEParameter(EParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Ecore_plus_logicSwitch
