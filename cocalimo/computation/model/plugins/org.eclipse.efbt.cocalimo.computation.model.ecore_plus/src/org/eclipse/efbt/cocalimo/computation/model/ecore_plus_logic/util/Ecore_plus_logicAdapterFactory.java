/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.util;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.*;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.ETypedElement;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage
 * @generated
 */
public class Ecore_plus_logicAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Ecore_plus_logicPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Ecore_plus_logicPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Ecore_plus_logicSwitch<Adapter> modelSwitch =
		new Ecore_plus_logicSwitch<Adapter>() {
			@Override
			public Adapter caseDerivedEntity(DerivedEntity object) {
				return createDerivedEntityAdapter();
			}
			@Override
			public Adapter caseIntermediateDerivedEntity(IntermediateDerivedEntity object) {
				return createIntermediateDerivedEntityAdapter();
			}
			@Override
			public Adapter caseGeneratedEntity(GeneratedEntity object) {
				return createGeneratedEntityAdapter();
			}
			@Override
			public Adapter caseAggregateFunctionSpec(AggregateFunctionSpec object) {
				return createAggregateFunctionSpecAdapter();
			}
			@Override
			public Adapter caseBasicFunctionSpec(BasicFunctionSpec object) {
				return createBasicFunctionSpecAdapter();
			}
			@Override
			public Adapter caseBooleanOperation(BooleanOperation object) {
				return createBooleanOperationAdapter();
			}
			@Override
			public Adapter caseFunctionSpec(FunctionSpec object) {
				return createFunctionSpecAdapter();
			}
			@Override
			public Adapter caseFunctionSpecModule(FunctionSpecModule object) {
				return createFunctionSpecModuleAdapter();
			}
			@Override
			public Adapter caseParameter(Parameter object) {
				return createParameterAdapter();
			}
			@Override
			public Adapter caseMemberParameter(MemberParameter object) {
				return createMemberParameterAdapter();
			}
			@Override
			public Adapter caseEntityParameter(EntityParameter object) {
				return createEntityParameterAdapter();
			}
			@Override
			public Adapter caseValueParameter(ValueParameter object) {
				return createValueParameterAdapter();
			}
			@Override
			public Adapter caseBasicScenarioOperation(BasicScenarioOperation object) {
				return createBasicScenarioOperationAdapter();
			}
			@Override
			public Adapter caseFeatureParameter(FeatureParameter object) {
				return createFeatureParameterAdapter();
			}
			@Override
			public Adapter caseOperationParameter(OperationParameter object) {
				return createOperationParameterAdapter();
			}
			@Override
			public Adapter caseProxyOperationParameter(ProxyOperationParameter object) {
				return createProxyOperationParameterAdapter();
			}
			@Override
			public Adapter caseProxyFeatureParameter(ProxyFeatureParameter object) {
				return createProxyFeatureParameterAdapter();
			}
			@Override
			public Adapter caseCustomRowCreationApproach(CustomRowCreationApproach object) {
				return createCustomRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseFilterAndGroupToOneRowCreationApproach(FilterAndGroupToOneRowCreationApproach object) {
				return createFilterAndGroupToOneRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseOneToOneRowCreationApproach(OneToOneRowCreationApproach object) {
				return createOneToOneRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseFilterRowCreationApproach(FilterRowCreationApproach object) {
				return createFilterRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseRowCreationApproach(RowCreationApproach object) {
				return createRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseGroupByRowCreationApproach(GroupByRowCreationApproach object) {
				return createGroupByRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseRowJoinFunction(RowJoinFunction object) {
				return createRowJoinFunctionAdapter();
			}
			@Override
			public Adapter caseUnionRowCreationApproach(UnionRowCreationApproach object) {
				return createUnionRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseExplodeArrayOfStructsRowFunction(ExplodeArrayOfStructsRowFunction object) {
				return createExplodeArrayOfStructsRowFunctionAdapter();
			}
			@Override
			public Adapter caseDerivedEntityModule(DerivedEntityModule object) {
				return createDerivedEntityModuleAdapter();
			}
			@Override
			public Adapter caseIntermediateDerivedEntitiesModule(IntermediateDerivedEntitiesModule object) {
				return createIntermediateDerivedEntitiesModuleAdapter();
			}
			@Override
			public Adapter caseGeneratedEntityModule(GeneratedEntityModule object) {
				return createGeneratedEntityModuleAdapter();
			}
			@Override
			public Adapter caseEntityTable(EntityTable object) {
				return createEntityTableAdapter();
			}
			@Override
			public Adapter caseDerivedEntityTable(DerivedEntityTable object) {
				return createDerivedEntityTableAdapter();
			}
			@Override
			public Adapter caseBaseEntityTable(BaseEntityTable object) {
				return createBaseEntityTableAdapter();
			}
			@Override
			public Adapter caseOperation(Operation object) {
				return createOperationAdapter();
			}
			@Override
			public Adapter caseBasicOperation(BasicOperation object) {
				return createBasicOperationAdapter();
			}
			@Override
			public Adapter caseAggregateOperation(AggregateOperation object) {
				return createAggregateOperationAdapter();
			}
			@Override
			public Adapter caseEntityFromSourceOperation(EntityFromSourceOperation object) {
				return createEntityFromSourceOperationAdapter();
			}
			@Override
			public Adapter caseAttributeFromEntityOperation(AttributeFromEntityOperation object) {
				return createAttributeFromEntityOperationAdapter();
			}
			@Override
			public Adapter caseCellsParameter(CellsParameter object) {
				return createCellsParameterAdapter();
			}
			@Override
			public Adapter caseBasicOperationScenarioSet(BasicOperationScenarioSet object) {
				return createBasicOperationScenarioSetAdapter();
			}
			@Override
			public Adapter caseBaseEntityTableModule(BaseEntityTableModule object) {
				return createBaseEntityTableModuleAdapter();
			}
			@Override
			public Adapter caseDerivedEntityTableModule(DerivedEntityTableModule object) {
				return createDerivedEntityTableModuleAdapter();
			}
			@Override
			public Adapter caseEModelElement(EModelElement object) {
				return createEModelElementAdapter();
			}
			@Override
			public Adapter caseENamedElement(ENamedElement object) {
				return createENamedElementAdapter();
			}
			@Override
			public Adapter caseEClassifier(EClassifier object) {
				return createEClassifierAdapter();
			}
			@Override
			public Adapter caseEClass(EClass object) {
				return createEClassAdapter();
			}
			@Override
			public Adapter caseEntity(Entity object) {
				return createEntityAdapter();
			}
			@Override
			public Adapter caseETypedElement(ETypedElement object) {
				return createETypedElementAdapter();
			}
			@Override
			public Adapter caseEOperation(EOperation object) {
				return createEOperationAdapter();
			}
			@Override
			public Adapter caseEParameter(EParameter object) {
				return createEParameterAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity
	 * @generated
	 */
	public Adapter createDerivedEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity <em>Intermediate Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity
	 * @generated
	 */
	public Adapter createIntermediateDerivedEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity
	 * @generated
	 */
	public Adapter createGeneratedEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec <em>Aggregate Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec
	 * @generated
	 */
	public Adapter createAggregateFunctionSpecAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec <em>Basic Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec
	 * @generated
	 */
	public Adapter createBasicFunctionSpecAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation <em>Boolean Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation
	 * @generated
	 */
	public Adapter createBooleanOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec
	 * @generated
	 */
	public Adapter createFunctionSpecAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule <em>Function Spec Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule
	 * @generated
	 */
	public Adapter createFunctionSpecModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter
	 * @generated
	 */
	public Adapter createParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter <em>Member Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter
	 * @generated
	 */
	public Adapter createMemberParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter <em>Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter
	 * @generated
	 */
	public Adapter createEntityParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter <em>Value Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter
	 * @generated
	 */
	public Adapter createValueParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation <em>Basic Scenario Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation
	 * @generated
	 */
	public Adapter createBasicScenarioOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter <em>Feature Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter
	 * @generated
	 */
	public Adapter createFeatureParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter <em>Operation Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter
	 * @generated
	 */
	public Adapter createOperationParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter <em>Proxy Operation Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter
	 * @generated
	 */
	public Adapter createProxyOperationParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter <em>Proxy Feature Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter
	 * @generated
	 */
	public Adapter createProxyFeatureParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach <em>Custom Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach
	 * @generated
	 */
	public Adapter createCustomRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach <em>Filter And Group To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach
	 * @generated
	 */
	public Adapter createFilterAndGroupToOneRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach <em>One To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach
	 * @generated
	 */
	public Adapter createOneToOneRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach <em>Filter Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach
	 * @generated
	 */
	public Adapter createFilterRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach <em>Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach
	 * @generated
	 */
	public Adapter createRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach <em>Group By Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach
	 * @generated
	 */
	public Adapter createGroupByRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction <em>Row Join Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction
	 * @generated
	 */
	public Adapter createRowJoinFunctionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach <em>Union Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach
	 * @generated
	 */
	public Adapter createUnionRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction <em>Explode Array Of Structs Row Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction
	 * @generated
	 */
	public Adapter createExplodeArrayOfStructsRowFunctionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule <em>Derived Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule
	 * @generated
	 */
	public Adapter createDerivedEntityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule <em>Intermediate Derived Entities Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule
	 * @generated
	 */
	public Adapter createIntermediateDerivedEntitiesModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule <em>Generated Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule
	 * @generated
	 */
	public Adapter createGeneratedEntityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable <em>Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable
	 * @generated
	 */
	public Adapter createEntityTableAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable <em>Derived Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable
	 * @generated
	 */
	public Adapter createDerivedEntityTableAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable <em>Base Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable
	 * @generated
	 */
	public Adapter createBaseEntityTableAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Operation <em>Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Operation
	 * @generated
	 */
	public Adapter createOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation <em>Basic Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation
	 * @generated
	 */
	public Adapter createBasicOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation <em>Aggregate Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation
	 * @generated
	 */
	public Adapter createAggregateOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityFromSourceOperation <em>Entity From Source Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityFromSourceOperation
	 * @generated
	 */
	public Adapter createEntityFromSourceOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityOperation <em>Attribute From Entity Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityOperation
	 * @generated
	 */
	public Adapter createAttributeFromEntityOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CellsParameter <em>Cells Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CellsParameter
	 * @generated
	 */
	public Adapter createCellsParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet <em>Basic Operation Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet
	 * @generated
	 */
	public Adapter createBasicOperationScenarioSetAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule <em>Base Entity Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule
	 * @generated
	 */
	public Adapter createBaseEntityTableModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule <em>Derived Entity Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule
	 * @generated
	 */
	public Adapter createDerivedEntityTableModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EModelElement <em>EModel Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EModelElement
	 * @generated
	 */
	public Adapter createEModelElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.ENamedElement <em>ENamed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.ENamedElement
	 * @generated
	 */
	public Adapter createENamedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EClassifier <em>EClassifier</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EClassifier
	 * @generated
	 */
	public Adapter createEClassifierAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EClass <em>EClass</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EClass
	 * @generated
	 */
	public Adapter createEClassAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity
	 * @generated
	 */
	public Adapter createEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.ETypedElement <em>ETyped Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.ETypedElement
	 * @generated
	 */
	public Adapter createETypedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EParameter <em>EParameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EParameter
	 * @generated
	 */
	public Adapter createEParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EOperation <em>EOperation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EOperation
	 * @generated
	 */
	public Adapter createEOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Ecore_plus_logicAdapterFactory
