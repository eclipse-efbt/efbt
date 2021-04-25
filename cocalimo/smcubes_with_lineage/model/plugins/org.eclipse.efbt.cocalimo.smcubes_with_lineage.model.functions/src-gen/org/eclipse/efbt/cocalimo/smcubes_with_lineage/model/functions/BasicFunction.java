/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A function which is not responsible for aggregating values from multiple rows in the same source cube
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getBasicFunction()
 * @model extendedMetaData="name='BasicFunction' kind='elementOnly'"
 * @generated
 */
public interface BasicFunction extends Function {
	/**
	 * Returns the value of the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The function specification
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Function Spec</em>' reference.
	 * @see #setFunctionSpec(BasicFunctionSpec)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getBasicFunction_FunctionSpec()
	 * @model extendedMetaData="kind='attribute' name='functionSpec'"
	 * @generated
	 */
	BasicFunctionSpec getFunctionSpec();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction#getFunctionSpec <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec</em>' reference.
	 * @see #getFunctionSpec()
	 * @generated
	 */
	void setFunctionSpec(BasicFunctionSpec value);

} // BasicFunction
