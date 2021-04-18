/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.util;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

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
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage
 * @generated
 */
public class FunctionsSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static FunctionsPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionsSwitch()
	{
		if (modelPackage == null) {
			modelPackage = FunctionsPackage.eINSTANCE;
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
	protected boolean isSwitchFor(EPackage ePackage)
	{
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
	protected T doSwitch(int classifierID, EObject theEObject)
	{
		switch (classifierID) {
			case FunctionsPackage.AGGREGATE_FUNCTION: {
				AggregateFunction aggregateFunction = (AggregateFunction)theEObject;
				T result = caseAggregateFunction(aggregateFunction);
				if (result == null) result = caseFunction(aggregateFunction);
				if (result == null) result = caseParameter(aggregateFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.AGGREGATE_FUNCTION_SPEC: {
				AggregateFunctionSpec aggregateFunctionSpec = (AggregateFunctionSpec)theEObject;
				T result = caseAggregateFunctionSpec(aggregateFunctionSpec);
				if (result == null) result = caseFunctionSpec(aggregateFunctionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.BASIC_FUNCTION: {
				BasicFunction basicFunction = (BasicFunction)theEObject;
				T result = caseBasicFunction(basicFunction);
				if (result == null) result = caseFunction(basicFunction);
				if (result == null) result = caseParameter(basicFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.BASIC_FUNCTION_SPEC: {
				BasicFunctionSpec basicFunctionSpec = (BasicFunctionSpec)theEObject;
				T result = caseBasicFunctionSpec(basicFunctionSpec);
				if (result == null) result = caseFunctionSpec(basicFunctionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.BOOLEAN_FUNCTION: {
				BooleanFunction booleanFunction = (BooleanFunction)theEObject;
				T result = caseBooleanFunction(booleanFunction);
				if (result == null) result = caseBasicFunction(booleanFunction);
				if (result == null) result = caseFunction(booleanFunction);
				if (result == null) result = caseParameter(booleanFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.FUNCTION: {
				Function function = (Function)theEObject;
				T result = caseFunction(function);
				if (result == null) result = caseParameter(function);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.FUNCTION_SPEC: {
				FunctionSpec functionSpec = (FunctionSpec)theEObject;
				T result = caseFunctionSpec(functionSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.FUNCTION_SPEC_MODULE: {
				FunctionSpecModule functionSpecModule = (FunctionSpecModule)theEObject;
				T result = caseFunctionSpecModule(functionSpecModule);
				if (result == null) result = caseModule(functionSpecModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.PARAMATER_SPEC: {
				ParamaterSpec paramaterSpec = (ParamaterSpec)theEObject;
				T result = caseParamaterSpec(paramaterSpec);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case FunctionsPackage.PARAMETER: {
				Parameter parameter = (Parameter)theEObject;
				T result = caseParameter(parameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
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
	public T caseAggregateFunction(AggregateFunction object)
	{
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
	public T caseAggregateFunctionSpec(AggregateFunctionSpec object)
	{
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
	public T caseBasicFunction(BasicFunction object)
	{
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
	public T caseBasicFunctionSpec(BasicFunctionSpec object)
	{
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
	public T caseBooleanFunction(BooleanFunction object)
	{
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
	public T caseFunction(Function object)
	{
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
	public T caseFunctionSpec(FunctionSpec object)
	{
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
	public T caseFunctionSpecModule(FunctionSpecModule object)
	{
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
	public T caseParamaterSpec(ParamaterSpec object)
	{
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
	public T caseParameter(Parameter object)
	{
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
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object)
	{
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
	public T defaultCase(EObject object)
	{
		return null;
	}

} //FunctionsSwitch
