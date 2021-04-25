/**
 */
package org.eclipse.efbt.controller.core.model.platform_call;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage
 * @generated
 */
public interface Platform_callFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Platform_callFactory eINSTANCE = org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Platform Call</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Platform Call</em>'.
	 * @generated
	 */
	PlatformCall createPlatformCall();

	/**
	 * Returns a new object of class '<em>Platform Call Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Platform Call Module</em>'.
	 * @generated
	 */
	PlatformCallModule createPlatformCallModule();

	/**
	 * Returns a new object of class '<em>Translate BIRD With Old Test Format To Cocason</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Translate BIRD With Old Test Format To Cocason</em>'.
	 * @generated
	 */
	TranslateBIRDWithOldTestFormatToCocason createTranslateBIRDWithOldTestFormatToCocason();

	/**
	 * Returns a new object of class '<em>Convert SQL Developer Model To Ecore</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Convert SQL Developer Model To Ecore</em>'.
	 * @generated
	 */
	ConvertSQLDeveloperModelToEcore createConvertSQLDeveloperModelToEcore();

	/**
	 * Returns a new object of class '<em>Translate BIRD With New Test Format To Cocason</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Translate BIRD With New Test Format To Cocason</em>'.
	 * @generated
	 */
	TranslateBIRDWithNewTestFormatToCocason createTranslateBIRDWithNewTestFormatToCocason();

	/**
	 * Returns a new object of class '<em>Program And Calls</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Program And Calls</em>'.
	 * @generated
	 */
	ProgramAndCalls createProgramAndCalls();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Platform_callPackage getPlatform_callPackage();

} //Platform_callFactory
