/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

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
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule#getLeafModuleToVersionedDTMViewsModules <em>Leaf Module To Versioned DTM Views Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule#getLeafModuleToVersionedCubeSchemaModules <em>Leaf Module To Versioned Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule#getLeafModuleToReportCellViewModules <em>Leaf Module To Report Cell View Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule#getScenarioToCodeConditions <em>Scenario To Code Conditions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule()
 * @model
 * @generated
 */
public interface DomainMappingModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Leaf Module To Versioned DTM Views Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Versioned DTM Views Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToVersionedDTMViewsModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToVersionedDTMViewsModule> getLeafModuleToVersionedDTMViewsModules();

	/**
	 * Returns the value of the '<em><b>Leaf Module To Versioned Cube Schema Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Versioned Cube Schema Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToVersionedCubeSchemaModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToVersionedCubeSchemaModule> getLeafModuleToVersionedCubeSchemaModules();

	/**
	 * Returns the value of the '<em><b>Leaf Module To Report Cell View Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToReportCellViewModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Leaf Module To Report Cell View Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_LeafModuleToReportCellViewModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<LeafModuleToReportCellViewModule> getLeafModuleToReportCellViewModules();

	/**
	 * Returns the value of the '<em><b>Scenario To Code Conditions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ScenarioToCodeConditions}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario To Code Conditions</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDomainMappingModule_ScenarioToCodeConditions()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioToCodeConditions> getScenarioToCodeConditions();

} // DomainMappingModule
