/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension;

import org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Domain Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module storing a list of Domains
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.DomainModule#getDomains <em>Domains</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getDomainModule()
 * @model extendedMetaData="name='DomainModule' kind='elementOnly'"
 * @generated
 */
public interface DomainModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Domains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of domains contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Domains</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getDomainModule_Domains()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='domains'"
	 * @generated
	 */
	EList<DOMAIN> getDomains();

} // DomainModule
