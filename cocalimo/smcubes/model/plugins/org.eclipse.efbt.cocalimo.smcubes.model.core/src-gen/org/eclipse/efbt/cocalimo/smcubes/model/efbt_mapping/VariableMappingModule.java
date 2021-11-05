/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing SMCubes VariableMappings
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.VariableMappingModule#getVariableMappings <em>Variable Mappings</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getVariableMappingModule()
 * @model
 * @generated
 */
public interface VariableMappingModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Variable Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  VariableMappings
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variable Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getVariableMappingModule_VariableMappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<VARIABLE_MAPPING> getVariableMappings();

} // VariableMappingModule
