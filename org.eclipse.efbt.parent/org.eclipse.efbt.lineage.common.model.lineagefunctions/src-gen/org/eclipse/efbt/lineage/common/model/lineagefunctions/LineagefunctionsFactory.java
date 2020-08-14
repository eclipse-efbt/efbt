/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage
 * @generated
 */
public interface LineagefunctionsFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	LineagefunctionsFactory eINSTANCE = org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Member Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Parameter</em>'.
	 * @generated
	 */
	MemberParameter createMemberParameter();

	/**
	 * Returns a new object of class '<em>Resolved Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Resolved Cube Column Parameter</em>'.
	 * @generated
	 */
	ResolvedCubeColumnParameter createResolvedCubeColumnParameter();

	/**
	 * Returns a new object of class '<em>Cube Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Column</em>'.
	 * @generated
	 */
	CubeColumn createCubeColumn();

	/**
	 * Returns a new object of class '<em>Speculative Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Speculative Cube Column Parameter</em>'.
	 * @generated
	 */
	SpeculativeCubeColumnParameter createSpeculativeCubeColumnParameter();

	/**
	 * Returns a new object of class '<em>Value Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Value Parameter</em>'.
	 * @generated
	 */
	ValueParameter createValueParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	LineagefunctionsPackage getLineagefunctionsPackage();

} //LineagefunctionsFactory
