/**
 */
package org.eclipse.efbt.openregspecs.model.core;

import java.util.Date;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VARIABLE SET ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#isIs_flow <em>Is flow</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION()
 * @model extendedMetaData="name='VARIABLE_SET_ENUMERATION' kind='empty'"
 * @generated
 */
public interface VARIABLE_SET_ENUMERATION extends EObject {
	/**
	 * Returns the value of the '<em><b>Is flow</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is flow</em>' attribute.
	 * @see #setIs_flow(boolean)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Is_flow()
	 * @model extendedMetaData="kind='attribute' name='is_flow'"
	 * @generated
	 */
	boolean isIs_flow();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#isIs_flow <em>Is flow</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is flow</em>' attribute.
	 * @see #isIs_flow()
	 * @generated
	 */
	void setIs_flow(boolean value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain id</em>' reference.
	 * @see #setSubdomain_id(SUBDOMAIN)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Subdomain_id()
	 * @model extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	SUBDOMAIN getSubdomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain id</em>' reference.
	 * @see #getSubdomain_id()
	 * @generated
	 */
	void setSubdomain_id(SUBDOMAIN value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getValid_from <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid from</em>' attribute.
	 * @see #getValid_from()
	 * @generated
	 */
	void setValid_from(Date value);

	/**
	 * Returns the value of the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid to</em>' attribute.
	 * @see #setValid_to(Date)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

	/**
	 * Returns the value of the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable id</em>' reference.
	 * @see #setVariable_id(VARIABLE)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Variable_id()
	 * @model extendedMetaData="kind='attribute' name='variable_id'"
	 * @generated
	 */
	VARIABLE getVariable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getVariable_id <em>Variable id</em>}' reference.
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
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getVARIABLE_SET_ENUMERATION_Variable_set_id()
	 * @model extendedMetaData="kind='attribute' name='variable_set_id'"
	 * @generated
	 */
	VARIABLE_SET getVariable_set_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION#getVariable_set_id <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable set id</em>' reference.
	 * @see #getVariable_set_id()
	 * @generated
	 */
	void setVariable_set_id(VARIABLE_SET value);

} // VARIABLE_SET_ENUMERATION
