/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.util;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.*;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EParameter;
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
			case Ecore_plus_logicPackage.AGGREGATE_FUNCTION_SPEC: {
				AggregateFunctionSpec aggregateFunctionSpec = (AggregateFunctionSpec)theEObject;
				T result = caseAggregateFunctionSpec(aggregateFunctionSpec);
				if (result == null) result = caseFunctionSpec(aggregateFunctionSpec);
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
			case Ecore_plus_logicPackage.BOOLEAN_OPERATION: {
				BooleanOperation booleanOperation = (BooleanOperation)theEObject;
				T result = caseBooleanOperation(booleanOperation);
				if (result == null) result = caseBasicOperation(booleanOperation);
				if (result == null) result = caseOperation(booleanOperation);
				if (result == null) result = caseEOperation(booleanOperation);
				if (result == null) result = caseParameter(booleanOperation);
				if (result == null) result = caseEParameter(booleanOperation);
				if (result == null) result = caseETypedElement(booleanOperation);
				if (result == null) result = caseENamedElement(booleanOperation);
				if (result == null) result = caseEModelElement(booleanOperation);
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
			case Ecore_plus_logicPackage.PARAMETER: {
				Parameter parameter = (Parameter)theEObject;
				T result = caseParameter(parameter);
				if (result == null) result = caseEParameter(parameter);
				if (result == null) result = caseETypedElement(parameter);
				if (result == null) result = caseENamedElement(parameter);
				if (result == null) result = caseEModelElement(parameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.MEMBER_PARAMETER: {
				MemberParameter memberParameter = (MemberParameter)theEObject;
				T result = caseMemberParameter(memberParameter);
				if (result == null) result = caseParameter(memberParameter);
				if (result == null) result = caseEParameter(memberParameter);
				if (result == null) result = caseETypedElement(memberParameter);
				if (result == null) result = caseENamedElement(memberParameter);
				if (result == null) result = caseEModelElement(memberParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ENTITY_PARAMETER: {
				EntityParameter entityParameter = (EntityParameter)theEObject;
				T result = caseEntityParameter(entityParameter);
				if (result == null) result = caseParameter(entityParameter);
				if (result == null) result = caseEParameter(entityParameter);
				if (result == null) result = caseETypedElement(entityParameter);
				if (result == null) result = caseENamedElement(entityParameter);
				if (result == null) result = caseEModelElement(entityParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.VALUE_PARAMETER: {
				ValueParameter valueParameter = (ValueParameter)theEObject;
				T result = caseValueParameter(valueParameter);
				if (result == null) result = caseParameter(valueParameter);
				if (result == null) result = caseEParameter(valueParameter);
				if (result == null) result = caseETypedElement(valueParameter);
				if (result == null) result = caseENamedElement(valueParameter);
				if (result == null) result = caseEModelElement(valueParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION: {
				BasicScenarioOperation basicScenarioOperation = (BasicScenarioOperation)theEObject;
				T result = caseBasicScenarioOperation(basicScenarioOperation);
				if (result == null) result = caseOperation(basicScenarioOperation);
				if (result == null) result = caseEOperation(basicScenarioOperation);
				if (result == null) result = caseParameter(basicScenarioOperation);
				if (result == null) result = caseEParameter(basicScenarioOperation);
				if (result == null) result = caseETypedElement(basicScenarioOperation);
				if (result == null) result = caseENamedElement(basicScenarioOperation);
				if (result == null) result = caseEModelElement(basicScenarioOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FEATURE_PARAMETER: {
				FeatureParameter featureParameter = (FeatureParameter)theEObject;
				T result = caseFeatureParameter(featureParameter);
				if (result == null) result = caseParameter(featureParameter);
				if (result == null) result = caseEParameter(featureParameter);
				if (result == null) result = caseETypedElement(featureParameter);
				if (result == null) result = caseENamedElement(featureParameter);
				if (result == null) result = caseEModelElement(featureParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.OPERATION_PARAMETER: {
				OperationParameter operationParameter = (OperationParameter)theEObject;
				T result = caseOperationParameter(operationParameter);
				if (result == null) result = caseParameter(operationParameter);
				if (result == null) result = caseEParameter(operationParameter);
				if (result == null) result = caseETypedElement(operationParameter);
				if (result == null) result = caseENamedElement(operationParameter);
				if (result == null) result = caseEModelElement(operationParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PROXY_OPERATION_PARAMETER: {
				ProxyOperationParameter proxyOperationParameter = (ProxyOperationParameter)theEObject;
				T result = caseProxyOperationParameter(proxyOperationParameter);
				if (result == null) result = caseParameter(proxyOperationParameter);
				if (result == null) result = caseEParameter(proxyOperationParameter);
				if (result == null) result = caseETypedElement(proxyOperationParameter);
				if (result == null) result = caseENamedElement(proxyOperationParameter);
				if (result == null) result = caseEModelElement(proxyOperationParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.PROXY_FEATURE_PARAMETER: {
				ProxyFeatureParameter proxyFeatureParameter = (ProxyFeatureParameter)theEObject;
				T result = caseProxyFeatureParameter(proxyFeatureParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.CUSTOM_ROW_CREATION_APPROACH: {
				CustomRowCreationApproach customRowCreationApproach = (CustomRowCreationApproach)theEObject;
				T result = caseCustomRowCreationApproach(customRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(customRowCreationApproach);
				if (result == null) result = caseEOperation(customRowCreationApproach);
				if (result == null) result = caseETypedElement(customRowCreationApproach);
				if (result == null) result = caseENamedElement(customRowCreationApproach);
				if (result == null) result = caseEModelElement(customRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: {
				FilterAndGroupToOneRowCreationApproach filterAndGroupToOneRowCreationApproach = (FilterAndGroupToOneRowCreationApproach)theEObject;
				T result = caseFilterAndGroupToOneRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseEOperation(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseETypedElement(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseENamedElement(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseEModelElement(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: {
				OneToOneRowCreationApproach oneToOneRowCreationApproach = (OneToOneRowCreationApproach)theEObject;
				T result = caseOneToOneRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = caseEOperation(oneToOneRowCreationApproach);
				if (result == null) result = caseETypedElement(oneToOneRowCreationApproach);
				if (result == null) result = caseENamedElement(oneToOneRowCreationApproach);
				if (result == null) result = caseEModelElement(oneToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH: {
				FilterRowCreationApproach filterRowCreationApproach = (FilterRowCreationApproach)theEObject;
				T result = caseFilterRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = caseEOperation(filterRowCreationApproach);
				if (result == null) result = caseETypedElement(filterRowCreationApproach);
				if (result == null) result = caseENamedElement(filterRowCreationApproach);
				if (result == null) result = caseEModelElement(filterRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH: {
				RowCreationApproach rowCreationApproach = (RowCreationApproach)theEObject;
				T result = caseRowCreationApproach(rowCreationApproach);
				if (result == null) result = caseEOperation(rowCreationApproach);
				if (result == null) result = caseETypedElement(rowCreationApproach);
				if (result == null) result = caseENamedElement(rowCreationApproach);
				if (result == null) result = caseEModelElement(rowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: {
				GroupByRowCreationApproach groupByRowCreationApproach = (GroupByRowCreationApproach)theEObject;
				T result = caseGroupByRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = caseEOperation(groupByRowCreationApproach);
				if (result == null) result = caseETypedElement(groupByRowCreationApproach);
				if (result == null) result = caseENamedElement(groupByRowCreationApproach);
				if (result == null) result = caseEModelElement(groupByRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION: {
				RowJoinFunction rowJoinFunction = (RowJoinFunction)theEObject;
				T result = caseRowJoinFunction(rowJoinFunction);
				if (result == null) result = caseRowCreationApproach(rowJoinFunction);
				if (result == null) result = caseEOperation(rowJoinFunction);
				if (result == null) result = caseETypedElement(rowJoinFunction);
				if (result == null) result = caseENamedElement(rowJoinFunction);
				if (result == null) result = caseEModelElement(rowJoinFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.UNION_ROW_CREATION_APPROACH: {
				UnionRowCreationApproach unionRowCreationApproach = (UnionRowCreationApproach)theEObject;
				T result = caseUnionRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = caseEOperation(unionRowCreationApproach);
				if (result == null) result = caseETypedElement(unionRowCreationApproach);
				if (result == null) result = caseENamedElement(unionRowCreationApproach);
				if (result == null) result = caseEModelElement(unionRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION: {
				ExplodeArrayOfStructsRowFunction explodeArrayOfStructsRowFunction = (ExplodeArrayOfStructsRowFunction)theEObject;
				T result = caseExplodeArrayOfStructsRowFunction(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseRowCreationApproach(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseEOperation(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseETypedElement(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseENamedElement(explodeArrayOfStructsRowFunction);
				if (result == null) result = caseEModelElement(explodeArrayOfStructsRowFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.DERIVED_ENTITY_MODULE: {
				DerivedEntityModule derivedEntityModule = (DerivedEntityModule)theEObject;
				T result = caseDerivedEntityModule(derivedEntityModule);
				if (result == null) result = caseModule(derivedEntityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.INTERMEDIATE_DERIVED_ENTITIES_MODULE: {
				IntermediateDerivedEntitiesModule intermediateDerivedEntitiesModule = (IntermediateDerivedEntitiesModule)theEObject;
				T result = caseIntermediateDerivedEntitiesModule(intermediateDerivedEntitiesModule);
				if (result == null) result = caseModule(intermediateDerivedEntitiesModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.GENERATED_ENTITY_MODULE: {
				GeneratedEntityModule generatedEntityModule = (GeneratedEntityModule)theEObject;
				T result = caseGeneratedEntityModule(generatedEntityModule);
				if (result == null) result = caseModule(generatedEntityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ENTITY_TABLE: {
				EntityTable entityTable = (EntityTable)theEObject;
				T result = caseEntityTable(entityTable);
				if (result == null) result = caseEntity(entityTable);
				if (result == null) result = caseEClass(entityTable);
				if (result == null) result = caseEClassifier(entityTable);
				if (result == null) result = caseENamedElement(entityTable);
				if (result == null) result = caseEModelElement(entityTable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE: {
				DerivedEntityTable derivedEntityTable = (DerivedEntityTable)theEObject;
				T result = caseDerivedEntityTable(derivedEntityTable);
				if (result == null) result = caseEntityTable(derivedEntityTable);
				if (result == null) result = caseEntity(derivedEntityTable);
				if (result == null) result = caseEClass(derivedEntityTable);
				if (result == null) result = caseEClassifier(derivedEntityTable);
				if (result == null) result = caseENamedElement(derivedEntityTable);
				if (result == null) result = caseEModelElement(derivedEntityTable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE: {
				BaseEntityTable baseEntityTable = (BaseEntityTable)theEObject;
				T result = caseBaseEntityTable(baseEntityTable);
				if (result == null) result = caseEntityTable(baseEntityTable);
				if (result == null) result = caseEntity(baseEntityTable);
				if (result == null) result = caseEClass(baseEntityTable);
				if (result == null) result = caseEClassifier(baseEntityTable);
				if (result == null) result = caseENamedElement(baseEntityTable);
				if (result == null) result = caseEModelElement(baseEntityTable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.OPERATION: {
				Operation operation = (Operation)theEObject;
				T result = caseOperation(operation);
				if (result == null) result = caseEOperation(operation);
				if (result == null) result = caseParameter(operation);
				if (result == null) result = caseEParameter(operation);
				if (result == null) result = caseETypedElement(operation);
				if (result == null) result = caseENamedElement(operation);
				if (result == null) result = caseEModelElement(operation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_OPERATION: {
				BasicOperation basicOperation = (BasicOperation)theEObject;
				T result = caseBasicOperation(basicOperation);
				if (result == null) result = caseOperation(basicOperation);
				if (result == null) result = caseEOperation(basicOperation);
				if (result == null) result = caseParameter(basicOperation);
				if (result == null) result = caseEParameter(basicOperation);
				if (result == null) result = caseETypedElement(basicOperation);
				if (result == null) result = caseENamedElement(basicOperation);
				if (result == null) result = caseEModelElement(basicOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION: {
				AggregateOperation aggregateOperation = (AggregateOperation)theEObject;
				T result = caseAggregateOperation(aggregateOperation);
				if (result == null) result = caseOperation(aggregateOperation);
				if (result == null) result = caseEOperation(aggregateOperation);
				if (result == null) result = caseParameter(aggregateOperation);
				if (result == null) result = caseEParameter(aggregateOperation);
				if (result == null) result = caseETypedElement(aggregateOperation);
				if (result == null) result = caseENamedElement(aggregateOperation);
				if (result == null) result = caseEModelElement(aggregateOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ENTITY_FROM_SOURCE_OPERATION: {
				EntityFromSourceOperation entityFromSourceOperation = (EntityFromSourceOperation)theEObject;
				T result = caseEntityFromSourceOperation(entityFromSourceOperation);
				if (result == null) result = caseOperation(entityFromSourceOperation);
				if (result == null) result = caseEOperation(entityFromSourceOperation);
				if (result == null) result = caseParameter(entityFromSourceOperation);
				if (result == null) result = caseEParameter(entityFromSourceOperation);
				if (result == null) result = caseETypedElement(entityFromSourceOperation);
				if (result == null) result = caseENamedElement(entityFromSourceOperation);
				if (result == null) result = caseEModelElement(entityFromSourceOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_OPERATION: {
				AttributeFromEntityOperation attributeFromEntityOperation = (AttributeFromEntityOperation)theEObject;
				T result = caseAttributeFromEntityOperation(attributeFromEntityOperation);
				if (result == null) result = caseOperation(attributeFromEntityOperation);
				if (result == null) result = caseEOperation(attributeFromEntityOperation);
				if (result == null) result = caseParameter(attributeFromEntityOperation);
				if (result == null) result = caseEParameter(attributeFromEntityOperation);
				if (result == null) result = caseETypedElement(attributeFromEntityOperation);
				if (result == null) result = caseENamedElement(attributeFromEntityOperation);
				if (result == null) result = caseEModelElement(attributeFromEntityOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.CELLS_PARAMETER: {
				CellsParameter cellsParameter = (CellsParameter)theEObject;
				T result = caseCellsParameter(cellsParameter);
				if (result == null) result = caseParameter(cellsParameter);
				if (result == null) result = caseEParameter(cellsParameter);
				if (result == null) result = caseETypedElement(cellsParameter);
				if (result == null) result = caseENamedElement(cellsParameter);
				if (result == null) result = caseEModelElement(cellsParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET: {
				BasicOperationScenarioSet basicOperationScenarioSet = (BasicOperationScenarioSet)theEObject;
				T result = caseBasicOperationScenarioSet(basicOperationScenarioSet);
				if (result == null) result = caseOperation(basicOperationScenarioSet);
				if (result == null) result = caseEOperation(basicOperationScenarioSet);
				if (result == null) result = caseParameter(basicOperationScenarioSet);
				if (result == null) result = caseEParameter(basicOperationScenarioSet);
				if (result == null) result = caseETypedElement(basicOperationScenarioSet);
				if (result == null) result = caseENamedElement(basicOperationScenarioSet);
				if (result == null) result = caseEModelElement(basicOperationScenarioSet);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.BASE_ENTITY_TABLE_MODULE: {
				BaseEntityTableModule baseEntityTableModule = (BaseEntityTableModule)theEObject;
				T result = caseBaseEntityTableModule(baseEntityTableModule);
				if (result == null) result = caseModule(baseEntityTableModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE_MODULE: {
				DerivedEntityTableModule derivedEntityTableModule = (DerivedEntityTableModule)theEObject;
				T result = caseDerivedEntityTableModule(derivedEntityTableModule);
				if (result == null) result = caseModule(derivedEntityTableModule);
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
	 * Returns the result of interpreting the object as an instance of '<em>Boolean Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Boolean Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBooleanOperation(BooleanOperation object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>Entity Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityParameter(EntityParameter object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>Basic Scenario Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Scenario Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicScenarioOperation(BasicScenarioOperation object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>Operation Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Operation Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOperationParameter(OperationParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Proxy Operation Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Proxy Operation Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseProxyOperationParameter(ProxyOperationParameter object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>Derived Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Entity Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedEntityModule(DerivedEntityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Intermediate Derived Entities Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Intermediate Derived Entities Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseIntermediateDerivedEntitiesModule(IntermediateDerivedEntitiesModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generated Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generated Entity Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGeneratedEntityModule(GeneratedEntityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity Table</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity Table</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityTable(EntityTable object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Entity Table</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Entity Table</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedEntityTable(DerivedEntityTable object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Entity Table</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Entity Table</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseEntityTable(BaseEntityTable object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOperation(Operation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicOperation(BasicOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateOperation(AggregateOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity From Source Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity From Source Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityFromSourceOperation(EntityFromSourceOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute From Entity Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute From Entity Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttributeFromEntityOperation(AttributeFromEntityOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cells Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cells Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCellsParameter(CellsParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Operation Scenario Set</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Operation Scenario Set</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicOperationScenarioSet(BasicOperationScenarioSet object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Entity Table Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Entity Table Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseEntityTableModule(BaseEntityTableModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Entity Table Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Entity Table Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedEntityTableModule(DerivedEntityTableModule object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>EOperation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EOperation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEOperation(EOperation object) {
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
