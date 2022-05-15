/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition;

import org.eclipse.efbt.openregspecs.model.core.MEMBER;
import org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN;
import org.eclipse.efbt.openregspecs.model.core.VARIABLE;
import org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>COMBINATION ITEM</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCOMBINATION_ITEM()
 * @model extendedMetaData="name='COMBINATION_ITEM' kind='empty'"
 * @generated
 */
public interface COMBINATION_ITEM extends EObject {
	/**
	 * Returns the value of the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' reference.
	 * @see #setMember_id(MEMBER)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCOMBINATION_ITEM_Member_id()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	MEMBER getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getMember_id <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' reference.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain id</em>' reference.
	 * @see #setSubdomain_id(SUBDOMAIN)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCOMBINATION_ITEM_Subdomain_id()
	 * @model extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	SUBDOMAIN getSubdomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getSubdomain_id <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain id</em>' reference.
	 * @see #getSubdomain_id()
	 * @generated
	 */
	void setSubdomain_id(SUBDOMAIN value);

	/**
	 * Returns the value of the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable id</em>' reference.
	 * @see #setVariable_id(VARIABLE)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCOMBINATION_ITEM_Variable_id()
	 * @model extendedMetaData="kind='attribute' name='variable_id'"
	 * @generated
	 */
	VARIABLE getVariable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getVariable_id <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable id</em>' reference.
	 * @see #getVariable_id()
	 * @generated
	 */
	void setVariable_id(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable set id</em>' reference.
	 * @see #setVariable_set_id(VARIABLE_SET)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCOMBINATION_ITEM_Variable_set_id()
	 * @model extendedMetaData="kind='attribute' name='variable_set_id'"
	 * @generated
	 */
	VARIABLE_SET getVariable_set_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM#getVariable_set_id <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable set id</em>' reference.
	 * @see #getVariable_set_id()
	 * @generated
	 */
	void setVariable_set_id(VARIABLE_SET value);

} // COMBINATION_ITEM
