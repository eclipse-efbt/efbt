/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aggregate Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A function which is  responsible for aggregating values from multiple rows in the same source cube
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getAggregateFunction()
 * @model extendedMetaData="name='AggregateFunction' kind='elementOnly'"
 * @generated
 */
public interface AggregateFunction extends Function
{
	/**
	 * Returns the value of the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The function specification
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Function Spec</em>' reference.
	 * @see #setFunctionSpec(AggregateFunctionSpec)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getAggregateFunction_FunctionSpec()
	 * @model extendedMetaData="kind='attribute' name='functionSpec'"
	 * @generated
	 */
	AggregateFunctionSpec getFunctionSpec();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction#getFunctionSpec <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec</em>' reference.
	 * @see #getFunctionSpec()
	 * @generated
	 */
	void setFunctionSpec(AggregateFunctionSpec value);

} // AggregateFunction
