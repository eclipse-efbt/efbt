/**
 */
package org.eclipse.efbt.controller.model.platform_call;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage
 * @generated
 */
public interface Platform_callFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Platform_callFactory eINSTANCE = org.eclipse.efbt.controller.model.platform_call.impl.Platform_callFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Compare Attribute Lineage Models</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Compare Attribute Lineage Models</em>'.
	 * @generated
	 */
	CompareAttributeLineageModels createCompareAttributeLineageModels();

	/**
	 * Returns a new object of class '<em>Is Not Subset</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Is Not Subset</em>'.
	 * @generated
	 */
	IsNotSubset createIsNotSubset();

	/**
	 * Returns a new object of class '<em>Is Subset</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Is Subset</em>'.
	 * @generated
	 */
	IsSubset createIsSubset();

	/**
	 * Returns a new object of class '<em>Subset Boolean</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Subset Boolean</em>'.
	 * @generated
	 */
	SubsetBoolean createSubsetBoolean();

	/**
	 * Returns a new object of class '<em>Execute Attribute Lineage Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Execute Attribute Lineage Model</em>'.
	 * @generated
	 */
	ExecuteAttributeLineageModel createExecuteAttributeLineageModel();

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
	 * Returns a new object of class '<em>Get Attribute Lineage Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Get Attribute Lineage Model</em>'.
	 * @generated
	 */
	GetAttributeLineageModel createGetAttributeLineageModel();

	/**
	 * Returns a new object of class '<em>Get Attribute Lineage For One Report Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Get Attribute Lineage For One Report Cell</em>'.
	 * @generated
	 */
	GetAttributeLineageForOneReportCell createGetAttributeLineageForOneReportCell();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Platform_callPackage getPlatform_callPackage();

} //Platform_callFactory
