/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cube Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing SMCubes CubeMappings
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.CubeMappingModule#getCubeMappings <em>Cube Mappings</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getCubeMappingModule()
 * @model
 * @generated
 */
public interface CubeMappingModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Cube Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeMappings
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getCubeMappingModule_CubeMappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<CUBE_MAPPING> getCubeMappings();

} // CubeMappingModule
