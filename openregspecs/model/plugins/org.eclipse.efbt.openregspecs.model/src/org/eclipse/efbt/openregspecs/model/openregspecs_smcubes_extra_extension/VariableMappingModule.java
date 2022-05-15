/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension;

import org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_MAPPING;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.VariableMappingModule#getVariableMappings <em>Variable Mappings</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getVariableMappingModule()
 * @model
 * @generated
 */
public interface VariableMappingModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Variable Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_MAPPING}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  VariableMappings
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variable Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage#getVariableMappingModule_VariableMappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<VARIABLE_MAPPING> getVariableMappings();

} // VariableMappingModule
