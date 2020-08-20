/**
 */
package org.eclipse.efbt.common.model.functions;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Function represents a function that takes parameters and returns a result.
 *       The parameters of a function may be other functions also.  
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.common.model.functions.Function#getParameters <em>Parameters</em>}</li>
 *   <li>{@link org.eclipse.efbt.common.model.functions.Function#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.common.model.functions.FunctionsPackage#getFunction()
 * @model extendedMetaData="name='Function' kind='elementOnly'"
 * @generated
 */
public interface Function extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Parameters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.common.model.functions.Parameter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The parameters of the function
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Parameters</em>' containment reference list.
	 * @see org.eclipse.efbt.common.model.functions.FunctionsPackage#getFunction_Parameters()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='parameters'"
	 * @generated
	 */
	EList<Parameter> getParameters();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the Parameter
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.common.model.functions.FunctionsPackage#getFunction_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.common.model.functions.Function#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // Function
