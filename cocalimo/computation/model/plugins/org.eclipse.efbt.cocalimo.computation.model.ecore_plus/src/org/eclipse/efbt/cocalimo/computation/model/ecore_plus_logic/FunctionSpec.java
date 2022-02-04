/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Function Spec</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Function Specification
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getFunctionalName <em>Functional Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getSqlName <em>Sql Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFunctionSpec()
 * @model extendedMetaData="name='FunctionSpec' kind='elementOnly'"
 * @generated
 */
public interface FunctionSpec extends EObject {
	/**
	 * Returns the value of the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * If the function spec represents a common function in a functional language like +, then this field holds that name.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functional Name</em>' attribute.
	 * @see #setFunctionalName(String)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFunctionSpec_FunctionalName()
	 * @model extendedMetaData="kind='attribute' name='functionalName'"
	 * @generated
	 */
	String getFunctionalName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getFunctionalName <em>Functional Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Functional Name</em>' attribute.
	 * @see #getFunctionalName()
	 * @generated
	 */
	void setFunctionalName(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The unique name of the function spec. 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFunctionSpec_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * If the function spec represents a common SQL function like SUM, then this field holds that name 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sql Name</em>' attribute.
	 * @see #setSqlName(String)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFunctionSpec_SqlName()
	 * @model extendedMetaData="kind='attribute' name='sqlName'"
	 * @generated
	 */
	String getSqlName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getSqlName <em>Sql Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sql Name</em>' attribute.
	 * @see #getSqlName()
	 * @generated
	 */
	void setSqlName(String value);

} // FunctionSpec
