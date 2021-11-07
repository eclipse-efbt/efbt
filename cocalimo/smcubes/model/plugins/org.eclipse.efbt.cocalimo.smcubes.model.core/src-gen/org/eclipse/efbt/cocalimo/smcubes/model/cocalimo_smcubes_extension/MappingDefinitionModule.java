/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Mapping Definition Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing SMCubes MappingDefintitions
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MappingDefinitionModule#getMappings <em>Mappings</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getMappingDefinitionModule()
 * @model
 * @generated
 */
public interface MappingDefinitionModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  MappingDefintitions
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getMappingDefinitionModule_Mappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<MAPPING_DEFINITION> getMappings();

} // MappingDefinitionModule
