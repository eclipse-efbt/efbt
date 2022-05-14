/**
 */
package org.eclipse.efbt.openregspecs.model.platform_call;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage
 * @generated
 */
public interface Platform_callFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Platform_callFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.platform_call.impl.Platform_callFactoryImpl.init();

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
	 * Returns a new object of class '<em>Import BIRD From MS Access</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Import BIRD From MS Access</em>'.
	 * @generated
	 */
	ImportBIRDFromMSAccess createImportBIRDFromMSAccess();

	/**
	 * Returns a new object of class '<em>Create Logical Transformation View For Scope</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Create Logical Transformation View For Scope</em>'.
	 * @generated
	 */
	CreateLogicalTransformationViewForScope createCreateLogicalTransformationViewForScope();

	/**
	 * Returns a new object of class '<em>Convert Data Meta Model To Ecore</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Convert Data Meta Model To Ecore</em>'.
	 * @generated
	 */
	ConvertDataMetaModelToEcore createConvertDataMetaModelToEcore();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Platform_callPackage getPlatform_callPackage();

} //Platform_callFactory
