/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSubDomainModule()
 * @model
 * @generated
 */
public interface SubDomainModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Subdomains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomains</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSubDomainModule_Subdomains()
	 * @model containment="true"
	 * @generated
	 */
	EList<SUBDOMAIN> getSubdomains();

} // SubDomainModule
