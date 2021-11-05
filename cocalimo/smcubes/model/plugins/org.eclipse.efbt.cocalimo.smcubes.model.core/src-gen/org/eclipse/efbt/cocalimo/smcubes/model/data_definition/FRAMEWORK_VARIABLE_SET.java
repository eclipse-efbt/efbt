/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>FRAMEWORK VARIABLE SET</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_VARIABLE_SET()
 * @model extendedMetaData="name='FRAMEWORK_VARIABLE_SET' kind='empty'"
 * @generated
 */
public interface FRAMEWORK_VARIABLE_SET extends EObject {
	/**
	 * Returns the value of the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Framework id</em>' reference.
	 * @see #setFramework_id(FRAMEWORK)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_VARIABLE_SET_Framework_id()
	 * @model extendedMetaData="kind='attribute' name='framework_id'"
	 * @generated
	 */
	FRAMEWORK getFramework_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getFramework_id <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Framework id</em>' reference.
	 * @see #getFramework_id()
	 * @generated
	 */
	void setFramework_id(FRAMEWORK value);

	/**
	 * Returns the value of the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable set id</em>' reference.
	 * @see #setVariable_set_id(VARIABLE_SET)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_VARIABLE_SET_Variable_set_id()
	 * @model extendedMetaData="kind='attribute' name='variable_set_id'"
	 * @generated
	 */
	VARIABLE_SET getVariable_set_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getVariable_set_id <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable set id</em>' reference.
	 * @see #getVariable_set_id()
	 * @generated
	 */
	void setVariable_set_id(VARIABLE_SET value);

} // FRAMEWORK_VARIABLE_SET
