/**
 */
package org.eclipse.efbt.cocalimo.core.model.module_management;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A grouping of related artifacts, persisted as a resource, with a version number, and a description, and a description of which Modules it depends upon (and importantly which version of those Modules).
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getDependencies <em>Dependencies</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getTheDescription <em>The Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLicense <em>License</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLongName <em>Long Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule()
 * @model abstract="true"
 *        extendedMetaData="name='Module' kind='elementOnly'"
 * @generated
 */
public interface Module extends EObject
{
	/**
	 * Returns the value of the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * These are the modules (and their version numbers) upon which this module is dependent.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Dependencies</em>' containment reference.
	 * @see #setDependencies(ModuleDependencies)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_Dependencies()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='dependencies'"
	 * @generated
	 */
	ModuleDependencies getDependencies();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getDependencies <em>Dependencies</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Dependencies</em>' containment reference.
	 * @see #getDependencies()
	 * @generated
	 */
	void setDependencies(ModuleDependencies value);

	/**
	 * Returns the value of the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The description of the module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>The Description</em>' attribute.
	 * @see #setTheDescription(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_TheDescription()
	 * @model extendedMetaData="kind='attribute' name='theDescription'"
	 * @generated
	 */
	String getTheDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getTheDescription <em>The Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>The Description</em>' attribute.
	 * @see #getTheDescription()
	 * @generated
	 */
	void setTheDescription(String value);

	/**
	 * Returns the value of the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The license under which this module is distributed.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>License</em>' attribute.
	 * @see #setLicense(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_License()
	 * @model extendedMetaData="kind='attribute' name='license'"
	 * @generated
	 */
	String getLicense();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLicense <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>License</em>' attribute.
	 * @see #getLicense()
	 * @generated
	 */
	void setLicense(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the module. This name acts as the modules identifier.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The version number of the module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Version</em>' attribute.
	 * @see #setVersion(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_Version()
	 * @model extendedMetaData="kind='attribute' name='version'"
	 * @generated
	 */
	String getVersion();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getVersion <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Version</em>' attribute.
	 * @see #getVersion()
	 * @generated
	 */
	void setVersion(String value);

	/**
	 * Returns the value of the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The long name associated with the module.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Long Name</em>' containment reference.
	 * @see #setLongName(ModuleLongName)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModule_LongName()
	 * @model containment="true"
	 * @generated
	 */
	ModuleLongName getLongName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLongName <em>Long Name</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Long Name</em>' containment reference.
	 * @see #getLongName()
	 * @generated
	 */
	void setLongName(ModuleLongName value);

} // Module
