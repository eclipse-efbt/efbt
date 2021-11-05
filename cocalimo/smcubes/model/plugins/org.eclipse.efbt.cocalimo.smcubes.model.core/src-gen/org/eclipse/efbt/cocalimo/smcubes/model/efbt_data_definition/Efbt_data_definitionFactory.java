/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage
 * @generated
 */
public interface Efbt_data_definitionFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_data_definitionFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Combination Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Combination Module</em>'.
	 * @generated
	 */
	CombinationModule createCombinationModule();

	/**
	 * Returns a new object of class '<em>Cube Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Module</em>'.
	 * @generated
	 */
	CubeModule createCubeModule();

	/**
	 * Returns a new object of class '<em>Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Domain Module</em>'.
	 * @generated
	 */
	DomainModule createDomainModule();

	/**
	 * Returns a new object of class '<em>Member Hierarchy Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Hierarchy Module</em>'.
	 * @generated
	 */
	MemberHierarchyModule createMemberHierarchyModule();

	/**
	 * Returns a new object of class '<em>Member Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Module</em>'.
	 * @generated
	 */
	MemberModule createMemberModule();

	/**
	 * Returns a new object of class '<em>Variable Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Variable Module</em>'.
	 * @generated
	 */
	VariableModule createVariableModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Efbt_data_definitionPackage getEfbt_data_definitionPackage();

} //Efbt_data_definitionFactory
