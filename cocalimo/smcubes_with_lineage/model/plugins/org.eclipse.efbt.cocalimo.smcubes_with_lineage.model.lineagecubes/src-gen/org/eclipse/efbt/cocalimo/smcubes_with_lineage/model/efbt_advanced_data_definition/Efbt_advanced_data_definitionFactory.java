/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage
 * @generated
 */
public interface Efbt_advanced_data_definitionFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_advanced_data_definitionFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Advanced Typed Variable</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Advanced Typed Variable</em>'.
	 * @generated
	 */
	AdvancedTypedVariable createAdvancedTypedVariable();

	/**
	 * Returns a new object of class '<em>Array Typed Variable</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Array Typed Variable</em>'.
	 * @generated
	 */
	ArrayTypedVariable createArrayTypedVariable();

	/**
	 * Returns a new object of class '<em>Array Typed Variable Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Array Typed Variable Module</em>'.
	 * @generated
	 */
	ArrayTypedVariableModule createArrayTypedVariableModule();

	/**
	 * Returns a new object of class '<em>Struct Typed Variable</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Struct Typed Variable</em>'.
	 * @generated
	 */
	StructTypedVariable createStructTypedVariable();

	/**
	 * Returns a new object of class '<em>Struct Typed Variables Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Struct Typed Variables Module</em>'.
	 * @generated
	 */
	StructTypedVariablesModule createStructTypedVariablesModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Efbt_advanced_data_definitionPackage getEfbt_advanced_data_definitionPackage();

} //Efbt_advanced_data_definitionFactory
