/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Operation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicOperation()
 * @model
 * @generated
 */
public interface BasicOperation extends Operation {
	/**
	 * Returns the value of the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Function Spec</em>' reference.
	 * @see #setFunctionSpec(BasicFunctionSpec)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicOperation_FunctionSpec()
	 * @model
	 * @generated
	 */
	BasicFunctionSpec getFunctionSpec();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation#getFunctionSpec <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec</em>' reference.
	 * @see #getFunctionSpec()
	 * @generated
	 */
	void setFunctionSpec(BasicFunctionSpec value);

} // BasicOperation
