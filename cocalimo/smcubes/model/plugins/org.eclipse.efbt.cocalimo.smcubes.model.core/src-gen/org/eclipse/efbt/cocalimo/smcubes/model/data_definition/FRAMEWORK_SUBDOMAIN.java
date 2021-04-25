/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>FRAMEWORK SUBDOMAIN</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_SUBDOMAIN()
 * @model extendedMetaData="name='FRAMEWORK_SUBDOMAIN' kind='empty'"
 * @generated
 */
public interface FRAMEWORK_SUBDOMAIN extends EObject
{
	/**
	 * Returns the value of the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Framework id</em>' reference.
	 * @see #setFramework_id(FRAMEWORK)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_SUBDOMAIN_Framework_id()
	 * @model extendedMetaData="kind='attribute' name='framework_id'"
	 * @generated
	 */
	FRAMEWORK getFramework_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getFramework_id <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Framework id</em>' reference.
	 * @see #getFramework_id()
	 * @generated
	 */
	void setFramework_id(FRAMEWORK value);

	/**
	 * Returns the value of the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain id</em>' reference.
	 * @see #setSubdomain_id(SUBDOMAIN)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getFRAMEWORK_SUBDOMAIN_Subdomain_id()
	 * @model extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	SUBDOMAIN getSubdomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain id</em>' reference.
	 * @see #getSubdomain_id()
	 * @generated
	 */
	void setSubdomain_id(SUBDOMAIN value);

} // FRAMEWORK_SUBDOMAIN
