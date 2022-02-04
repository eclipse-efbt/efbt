/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aggregate Operation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAggregateOperation()
 * @model
 * @generated
 */
public interface AggregateOperation extends Operation {
	/**
	 * Returns the value of the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Function Spec</em>' reference.
	 * @see #setFunctionSpec(AggregateFunctionSpec)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAggregateOperation_FunctionSpec()
	 * @model
	 * @generated
	 */
	AggregateFunctionSpec getFunctionSpec();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation#getFunctionSpec <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec</em>' reference.
	 * @see #getFunctionSpec()
	 * @generated
	 */
	void setFunctionSpec(AggregateFunctionSpec value);

} // AggregateOperation
