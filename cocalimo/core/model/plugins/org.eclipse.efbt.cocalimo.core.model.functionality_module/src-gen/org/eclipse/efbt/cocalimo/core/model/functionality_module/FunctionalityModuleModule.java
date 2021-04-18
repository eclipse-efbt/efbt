/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Functionality Module Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module of FunctionalityModules
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModules <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModuleTags <em>Functionality Module Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getSceanrioTags <em>Sceanrio Tags</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleModule()
 * @model extendedMetaData="name='FunctionalityModuleModule' kind='elementOnly'"
 * @generated
 */
public interface FunctionalityModuleModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Functionality Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  set of FunctionalityModules 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functionality Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleModule_FunctionalityModules()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='FunctionalityModules'"
	 * @generated
	 */
	EList<FunctionalityModule> getFunctionalityModules();

	/**
	 * Returns the value of the '<em><b>Functionality Module Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Functionality Module Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleModule_FunctionalityModuleTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<FunctionalityModuleTag> getFunctionalityModuleTags();

	/**
	 * Returns the value of the '<em><b>Sceanrio Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sceanrio Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleModule_SceanrioTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioTag> getSceanrioTags();

} // FunctionalityModuleModule
