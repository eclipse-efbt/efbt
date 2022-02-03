/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Sub Domain Enum</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum#getPrimaryEnum <em>Primary Enum</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum#getSubDomain <em>Sub Domain</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getSubDomainEnum()
 * @model
 * @generated
 */
public interface SubDomainEnum extends HeirarchicalEnum {
	/**
	 * Returns the value of the '<em><b>Primary Enum</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Primary Enum</em>' reference.
	 * @see #setPrimaryEnum(EnumeratedDomain)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getSubDomainEnum_PrimaryEnum()
	 * @model
	 * @generated
	 */
	EnumeratedDomain getPrimaryEnum();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum#getPrimaryEnum <em>Primary Enum</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Primary Enum</em>' reference.
	 * @see #getPrimaryEnum()
	 * @generated
	 */
	void setPrimaryEnum(EnumeratedDomain value);

	/**
	 * Returns the value of the '<em><b>Sub Domain</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Domain</em>' reference.
	 * @see #setSubDomain(SUBDOMAIN)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getSubDomainEnum_SubDomain()
	 * @model
	 * @generated
	 */
	SUBDOMAIN getSubDomain();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum#getSubDomain <em>Sub Domain</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sub Domain</em>' reference.
	 * @see #getSubDomain()
	 * @generated
	 */
	void setSubDomain(SUBDOMAIN value);

} // SubDomainEnum
