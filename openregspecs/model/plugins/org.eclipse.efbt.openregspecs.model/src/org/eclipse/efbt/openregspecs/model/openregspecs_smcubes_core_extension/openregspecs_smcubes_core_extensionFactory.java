/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage
 * @generated
 */
public interface openregspecs_smcubes_core_extensionFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	openregspecs_smcubes_core_extensionFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionFactoryImpl.init();

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
	 * Returns a new object of class '<em>Sub Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Sub Domain Module</em>'.
	 * @generated
	 */
	SubDomainModule createSubDomainModule();

	/**
	 * Returns a new object of class '<em>SM Cubes Core Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SM Cubes Core Model</em>'.
	 * @generated
	 */
	SMCubesCoreModel createSMCubesCoreModel();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	openregspecs_smcubes_core_extensionPackage getopenregspecs_smcubes_core_extensionPackage();

} //openregspecs_smcubes_core_extensionFactory
