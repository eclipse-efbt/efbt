/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Sub Domain Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainModule#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSubDomainModule()
 * @model
 * @generated
 */
public interface SubDomainModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Subdomains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomains</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSubDomainModule_Subdomains()
	 * @model containment="true"
	 * @generated
	 */
	EList<SUBDOMAIN> getSubdomains();

} // SubDomainModule
