/**
 */
package org.eclipse.efbt.openregspecs.model.core;

import java.util.Date;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SUBDOMAIN ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getSUBDOMAIN_ENUMERATION()
 * @model
 * @generated
 */
public interface SUBDOMAIN_ENUMERATION extends EObject {
	/**
	 * Returns the value of the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' reference.
	 * @see #setMember_id(MEMBER)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Member_id()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	MEMBER getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getMember_id <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' reference.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}' attribute.
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
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

} // SUBDOMAIN_ENUMERATION
