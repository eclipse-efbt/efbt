/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage
 * @generated
 */
public interface Efbt_mappingFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_mappingFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl.Efbt_mappingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Mapping Definition Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Mapping Definition Module</em>'.
	 * @generated
	 */
	MappingDefinitionModule createMappingDefinitionModule();

	/**
	 * Returns a new object of class '<em>Cube Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Mapping Module</em>'.
	 * @generated
	 */
	CubeMappingModule createCubeMappingModule();

	/**
	 * Returns a new object of class '<em>Member Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Mapping Module</em>'.
	 * @generated
	 */
	MemberMappingModule createMemberMappingModule();

	/**
	 * Returns a new object of class '<em>Variable Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Variable Mapping Module</em>'.
	 * @generated
	 */
	VariableMappingModule createVariableMappingModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Efbt_mappingPackage getEfbt_mappingPackage();

} //Efbt_mappingFactory
