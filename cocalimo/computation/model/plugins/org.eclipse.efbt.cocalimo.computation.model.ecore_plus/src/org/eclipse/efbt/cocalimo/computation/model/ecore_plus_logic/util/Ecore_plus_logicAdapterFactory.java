/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.util;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.*;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
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
			public Adapter caseAggregateFunction(AggregateFunction object) {
				return createAggregateFunctionAdapter();
			}
			@Override
			public Adapter caseAggregateFunctionSpec(AggregateFunctionSpec object) {
				return createAggregateFunctionSpecAdapter();
			}
			@Override
			public Adapter caseBasicFunction(BasicFunction object) {
				return createBasicFunctionAdapter();
			}
			@Override
			public Adapter caseBasicFunctionSpec(BasicFunctionSpec object) {
				return createBasicFunctionSpecAdapter();
			}
			@Override
			public Adapter caseBooleanFunction(BooleanFunction object) {
				return createBooleanFunctionAdapter();
			}
			@Override
			public Adapter caseFunction(Function object) {
				return createFunctionAdapter();
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
			public Adapter caseParamaterSpec(ParamaterSpec object) {
				return createParamaterSpecAdapter();
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
			public Adapter caseValueParameter(ValueParameter object) {
				return createValueParameterAdapter();
			}
			@Override
			public Adapter caseAggregateDerivedAttribute(AggregateDerivedAttribute object) {
				return createAggregateDerivedAttributeAdapter();
			}
			@Override
			public Adapter caseBasicDerivedAttribute(BasicDerivedAttribute object) {
				return createBasicDerivedAttributeAdapter();
			}
			@Override
			public Adapter caseBasicScenarioLogic(BasicScenarioLogic object) {
				return createBasicScenarioLogicAdapter();
			}
			@Override
			public Adapter caseDerivedAttribute(DerivedAttribute object) {
				return createDerivedAttributeAdapter();
			}
			@Override
			public Adapter caseDerivedStruct(DerivedStruct object) {
				return createDerivedStructAdapter();
			}
			@Override
			public Adapter caseBaseFeature(BaseFeature object) {
				return createBaseFeatureAdapter();
			}
			@Override
			public Adapter caseDerivedFeatureGroup(DerivedFeatureGroup object) {
				return createDerivedFeatureGroupAdapter();
			}
			@Override
			public Adapter caseFeatureParameter(FeatureParameter object) {
				return createFeatureParameterAdapter();
			}
			@Override
			public Adapter caseProxyFeatureParameter(ProxyFeatureParameter object) {
				return createProxyFeatureParameterAdapter();
			}
			@Override
			public Adapter caseGetAttributeFromEntityDerivedFeature(GetAttributeFromEntityDerivedFeature object) {
				return createGetAttributeFromEntityDerivedFeatureAdapter();
			}
			@Override
			public Adapter caseAttributeFromEntityParameter(AttributeFromEntityParameter object) {
				return createAttributeFromEntityParameterAdapter();
			}
			@Override
			public Adapter caseProxyAttributeFromEntityParameter(ProxyAttributeFromEntityParameter object) {
				return createProxyAttributeFromEntityParameterAdapter();
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
			public Adapter caseRowCreationApproachForEntity(RowCreationApproachForEntity object) {
				return createRowCreationApproachForEntityAdapter();
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
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter caseETypedElement(ETypedElement object) {
				return createETypedElementAdapter();
			}
			@Override
			public Adapter caseEStructuralFeature(EStructuralFeature object) {
				return createEStructuralFeatureAdapter();
			}
			@Override
			public Adapter caseEAttribute(EAttribute object) {
				return createEAttributeAdapter();
			}
			@Override
			public Adapter caseEReference(EReference object) {
				return createEReferenceAdapter();
			}
			@Override
			public Adapter caseEParameter(EParameter object) {
				return createEParameterAdapter();
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction
	 * @generated
	 */
	public Adapter createAggregateFunctionAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction
	 * @generated
	 */
	public Adapter createBasicFunctionAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction <em>Boolean Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction
	 * @generated
	 */
	public Adapter createBooleanFunctionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function
	 * @generated
	 */
	public Adapter createFunctionAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec <em>Paramater Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec
	 * @generated
	 */
	public Adapter createParamaterSpecAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute <em>Aggregate Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute
	 * @generated
	 */
	public Adapter createAggregateDerivedAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute <em>Basic Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute
	 * @generated
	 */
	public Adapter createBasicDerivedAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic <em>Basic Scenario Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic
	 * @generated
	 */
	public Adapter createBasicScenarioLogicAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute <em>Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute
	 * @generated
	 */
	public Adapter createDerivedAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct <em>Derived Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct
	 * @generated
	 */
	public Adapter createDerivedStructAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature <em>Base Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature
	 * @generated
	 */
	public Adapter createBaseFeatureAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup <em>Derived Feature Group</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup
	 * @generated
	 */
	public Adapter createDerivedFeatureGroupAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature <em>Get Attribute From Entity Derived Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature
	 * @generated
	 */
	public Adapter createGetAttributeFromEntityDerivedFeatureAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter <em>Attribute From Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter
	 * @generated
	 */
	public Adapter createAttributeFromEntityParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter <em>Proxy Attribute From Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter
	 * @generated
	 */
	public Adapter createProxyAttributeFromEntityParameterAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity
	 * @generated
	 */
	public Adapter createRowCreationApproachForEntityAdapter() {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EStructuralFeature <em>EStructural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EStructuralFeature
	 * @generated
	 */
	public Adapter createEStructuralFeatureAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EAttribute <em>EAttribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EAttribute
	 * @generated
	 */
	public Adapter createEAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.emf.ecore.EReference <em>EReference</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.emf.ecore.EReference
	 * @generated
	 */
	public Adapter createEReferenceAdapter() {
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
