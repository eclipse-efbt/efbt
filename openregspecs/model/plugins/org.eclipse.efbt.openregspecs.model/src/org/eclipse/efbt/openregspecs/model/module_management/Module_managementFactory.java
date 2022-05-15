/**
 */
package org.eclipse.efbt.openregspecs.model.module_management;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage
 * @generated
 */
public interface Module_managementFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Module_managementFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.module_management.impl.Module_managementFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module</em>'.
	 * @generated
	 */
	Module createModule();

	/**
	 * Returns a new object of class '<em>Module Long Name</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Long Name</em>'.
	 * @generated
	 */
	ModuleLongName createModuleLongName();

	/**
	 * Returns a new object of class '<em>Module Dependencies</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Dependencies</em>'.
	 * @generated
	 */
	ModuleDependencies createModuleDependencies();

	/**
	 * Returns a new object of class '<em>Module Dependency</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Dependency</em>'.
	 * @generated
	 */
	ModuleDependency createModuleDependency();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Module_managementPackage getModule_managementPackage();

} //Module_managementFactory
