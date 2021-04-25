/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Member Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing SMCubes MemberMappings
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MemberMappingModule#getMemberMappings <em>Member Mappings</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getMemberMappingModule()
 * @model
 * @generated
 */
public interface MemberMappingModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Member Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The MemberMappings
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Member Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage#getMemberMappingModule_MemberMappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<MEMBER_MAPPING> getMemberMappings();

} // MemberMappingModule
