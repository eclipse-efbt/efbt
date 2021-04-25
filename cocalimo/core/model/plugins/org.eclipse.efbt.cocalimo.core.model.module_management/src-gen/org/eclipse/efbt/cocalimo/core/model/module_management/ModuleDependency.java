/**
 */
package org.eclipse.efbt.cocalimo.core.model.module_management;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Module Dependency</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module dependency, including its version number and its long name
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleName <em>Module Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleVersion <em>Module Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getTheModule <em>The Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getLongName <em>Long Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModuleDependency()
 * @model extendedMetaData="name='ModuleDependency' kind='empty'"
 * @generated
 */
public interface ModuleDependency extends EObject
{
	/**
	 * Returns the value of the '<em><b>Module Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the dependant Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Module Name</em>' attribute.
	 * @see #setModuleName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModuleDependency_ModuleName()
	 * @model extendedMetaData="kind='attribute' name='moduleName'"
	 * @generated
	 */
	String getModuleName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleName <em>Module Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Module Name</em>' attribute.
	 * @see #getModuleName()
	 * @generated
	 */
	void setModuleName(String value);

	/**
	 * Returns the value of the '<em><b>Module Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The version of the dependent Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Module Version</em>' attribute.
	 * @see #setModuleVersion(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModuleDependency_ModuleVersion()
	 * @model extendedMetaData="kind='attribute' name='moduleVersion'"
	 * @generated
	 */
	String getModuleVersion();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleVersion <em>Module Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Module Version</em>' attribute.
	 * @see #getModuleVersion()
	 * @generated
	 */
	void setModuleVersion(String value);

	/**
	 * Returns the value of the '<em><b>The Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The dependent Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>The Module</em>' reference.
	 * @see #setTheModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModuleDependency_TheModule()
	 * @model extendedMetaData="kind='attribute' name='theModule'"
	 * @generated
	 */
	org.eclipse.efbt.cocalimo.core.model.module_management.Module getTheModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getTheModule <em>The Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>The Module</em>' reference.
	 * @see #getTheModule()
	 * @generated
	 */
	void setTheModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module value);

	/**
	 * Returns the value of the '<em><b>Long Name</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The dependent Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Long Name</em>' reference.
	 * @see #setLongName(ModuleLongName)
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage#getModuleDependency_LongName()
	 * @model
	 * @generated
	 */
	ModuleLongName getLongName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getLongName <em>Long Name</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Long Name</em>' reference.
	 * @see #getLongName()
	 * @generated
	 */
	void setLongName(ModuleLongName value);

} // ModuleDependency
