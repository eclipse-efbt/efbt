/**
 */
package entity_functions;

import functions.Parameter;

import org.eclipse.emf.ecore.EEnumLiteral;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Member Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter which is a Member of a Dimension, useful in Functions like 'Currency = USD' where Currency is a column and USD is a member of the Currency Dimension. 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link entity_functions.MemberParameter#getParam <em>Param</em>}</li>
 * </ul>
 *
 * @see entity_functions.Entity_functionsPackage#getMemberParameter()
 * @model extendedMetaData="name='MemberParameter' kind='empty'"
 * @generated
 */
public interface MemberParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The member
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Param</em>' reference.
	 * @see #setParam(EEnumLiteral)
	 * @see entity_functions.Entity_functionsPackage#getMemberParameter_Param()
	 * @model extendedMetaData="kind='attribute' name='param'"
	 * @generated
	 */
	EEnumLiteral getParam();

	/**
	 * Sets the value of the '{@link entity_functions.MemberParameter#getParam <em>Param</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Param</em>' reference.
	 * @see #getParam()
	 * @generated
	 */
	void setParam(EEnumLiteral value);

} // MemberParameter
