/**
 */
package functions;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Function Spec</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Function Specification for a Basic Function (as opposed to an AggregateFunction)
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link functions.BasicFunctionSpec#isIsInfix <em>Is Infix</em>}</li>
 * </ul>
 *
 * @see functions.FunctionsPackage#getBasicFunctionSpec()
 * @model extendedMetaData="name='BasicFunctionSpec' kind='elementOnly'"
 * @generated
 */
public interface BasicFunctionSpec extends FunctionSpec {
	/**
	 * Returns the value of the '<em><b>Is Infix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * true if the function is to be displayed in an infix fashion (like - or + functions)
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Is Infix</em>' attribute.
	 * @see #setIsInfix(boolean)
	 * @see functions.FunctionsPackage#getBasicFunctionSpec_IsInfix()
	 * @model extendedMetaData="kind='attribute' name='isInfix'"
	 * @generated
	 */
	boolean isIsInfix();

	/**
	 * Sets the value of the '{@link functions.BasicFunctionSpec#isIsInfix <em>Is Infix</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is Infix</em>' attribute.
	 * @see #isIsInfix()
	 * @generated
	 */
	void setIsInfix(boolean value);

} // BasicFunctionSpec
