/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage
 * @generated
 */
public interface FunctionsFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	FunctionsFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Aggregate Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Aggregate Function</em>'.
	 * @generated
	 */
	AggregateFunction createAggregateFunction();

	/**
	 * Returns a new object of class '<em>Aggregate Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Aggregate Function Spec</em>'.
	 * @generated
	 */
	AggregateFunctionSpec createAggregateFunctionSpec();

	/**
	 * Returns a new object of class '<em>Basic Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Basic Function</em>'.
	 * @generated
	 */
	BasicFunction createBasicFunction();

	/**
	 * Returns a new object of class '<em>Basic Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Basic Function Spec</em>'.
	 * @generated
	 */
	BasicFunctionSpec createBasicFunctionSpec();

	/**
	 * Returns a new object of class '<em>Boolean Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Boolean Function</em>'.
	 * @generated
	 */
	BooleanFunction createBooleanFunction();

	/**
	 * Returns a new object of class '<em>Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Function</em>'.
	 * @generated
	 */
	Function createFunction();

	/**
	 * Returns a new object of class '<em>Function Spec</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Function Spec</em>'.
	 * @generated
	 */
	FunctionSpec createFunctionSpec();

	/**
	 * Returns a new object of class '<em>Function Spec Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Function Spec Module</em>'.
	 * @generated
	 */
	FunctionSpecModule createFunctionSpecModule();

	/**
	 * Returns a new object of class '<em>Paramater Spec</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Paramater Spec</em>'.
	 * @generated
	 */
	ParamaterSpec createParamaterSpec();

	/**
	 * Returns a new object of class '<em>Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parameter</em>'.
	 * @generated
	 */
	Parameter createParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	FunctionsPackage getFunctionsPackage();

} //FunctionsFactory
