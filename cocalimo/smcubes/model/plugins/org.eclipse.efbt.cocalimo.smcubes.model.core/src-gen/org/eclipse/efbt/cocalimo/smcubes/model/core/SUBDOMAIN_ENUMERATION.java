/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Classifier;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SUBDOMAIN ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getMember_ids <em>Member ids</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION()
 * @model extendedMetaData="name='SUBDOMAIN_ENUMERATION' kind='empty'"
 * @generated
 */
public interface SUBDOMAIN_ENUMERATION extends Classifier {
	/**
	 * Returns the value of the '<em><b>Member ids</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member ids</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Member_ids()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	EList<MEMBER> getMember_ids();

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Subdomain_id()
	 * @model extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	SUBDOMAIN getSubdomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}' reference.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getSUBDOMAIN_ENUMERATION_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

} // SUBDOMAIN_ENUMERATION
