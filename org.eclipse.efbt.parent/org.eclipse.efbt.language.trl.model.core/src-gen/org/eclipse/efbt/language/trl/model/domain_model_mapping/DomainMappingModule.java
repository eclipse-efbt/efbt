/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Domain Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule#getLeafModuleToVersionedSQLViewsModules <em>Leaf Module To Versioned SQL Views Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule#getLeafModuleToVersionedCubeSchemaModules <em>Leaf Module To Versioned Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule#getLeafModuleToReportCellViewModules <em>Leaf Module To Report Cell View Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule#getScenarioToCodeConditions <em>Scenario To Code Conditions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule()
 * @model
 * @generated
 */
public interface DomainMappingModule extends org.eclipse.efbt.common.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Leaf Module To Versioned SQL Views Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToVersionedSQLViewsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Versioned SQL Views Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToVersionedSQLViewsModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToVersionedSQLViewsModule> getLeafModuleToVersionedSQLViewsModules();

	/**
	 * Returns the value of the '<em><b>Leaf Module To Versioned Cube Schema Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Versioned Cube Schema Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToVersionedCubeSchemaModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToVersionedCubeSchemaModule> getLeafModuleToVersionedCubeSchemaModules();

	/**
	 * Returns the value of the '<em><b>Leaf Module To Report Cell View Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToReportCellViewModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Report Cell View Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToReportCellViewModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToReportCellViewModule> getLeafModuleToReportCellViewModules();

	/**
	 * Returns the value of the '<em><b>Scenario To Code Conditions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario To Code Conditions</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_ScenarioToCodeConditions()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioToCodeConditions> getScenarioToCodeConditions();

} // DomainMappingModule
