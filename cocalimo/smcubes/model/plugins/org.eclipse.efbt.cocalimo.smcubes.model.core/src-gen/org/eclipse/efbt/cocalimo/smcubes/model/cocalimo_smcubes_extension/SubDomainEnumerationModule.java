/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Sub Domain Enumeration Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainEnumerationModule#getSubdomainEnumerations <em>Subdomain Enumerations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSubDomainEnumerationModule()
 * @model
 * @generated
 */
public interface SubDomainEnumerationModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Subdomain Enumerations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain Enumerations</em>' containment reference.
	 * @see #setSubdomainEnumerations(SUBDOMAIN_ENUMERATION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSubDomainEnumerationModule_SubdomainEnumerations()
	 * @model containment="true"
	 * @generated
	 */
	SUBDOMAIN_ENUMERATION getSubdomainEnumerations();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainEnumerationModule#getSubdomainEnumerations <em>Subdomain Enumerations</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain Enumerations</em>' containment reference.
	 * @see #getSubdomainEnumerations()
	 * @generated
	 */
	void setSubdomainEnumerations(SUBDOMAIN_ENUMERATION value);

} // SubDomainEnumerationModule
