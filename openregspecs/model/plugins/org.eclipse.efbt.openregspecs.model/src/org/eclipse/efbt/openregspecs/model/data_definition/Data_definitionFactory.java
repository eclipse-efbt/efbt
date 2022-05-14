/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage
 * @generated
 */
public interface Data_definitionFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_definitionFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.data_definition.impl.Data_definitionFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>COMBINATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>COMBINATION</em>'.
	 * @generated
	 */
	COMBINATION createCOMBINATION();

	/**
	 * Returns a new object of class '<em>COMBINATION ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>COMBINATION ITEM</em>'.
	 * @generated
	 */
	COMBINATION_ITEM createCOMBINATION_ITEM();

	/**
	 * Returns a new object of class '<em>CUBE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE</em>'.
	 * @generated
	 */
	CUBE createCUBE();

	/**
	 * Returns a new object of class '<em>CUBE GROUP</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE GROUP</em>'.
	 * @generated
	 */
	CUBE_GROUP createCUBE_GROUP();

	/**
	 * Returns a new object of class '<em>CUBE GROUP ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE GROUP ENUMERATION</em>'.
	 * @generated
	 */
	CUBE_GROUP_ENUMERATION createCUBE_GROUP_ENUMERATION();

	/**
	 * Returns a new object of class '<em>CUBE HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE HIERARCHY</em>'.
	 * @generated
	 */
	CUBE_HIERARCHY createCUBE_HIERARCHY();

	/**
	 * Returns a new object of class '<em>CUBE HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE HIERARCHY NODE</em>'.
	 * @generated
	 */
	CUBE_HIERARCHY_NODE createCUBE_HIERARCHY_NODE();

	/**
	 * Returns a new object of class '<em>CUBE RELATIONSHIP</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE RELATIONSHIP</em>'.
	 * @generated
	 */
	CUBE_RELATIONSHIP createCUBE_RELATIONSHIP();

	/**
	 * Returns a new object of class '<em>CUBE STRUCTURE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE STRUCTURE</em>'.
	 * @generated
	 */
	CUBE_STRUCTURE createCUBE_STRUCTURE();

	/**
	 * Returns a new object of class '<em>CUBE STRUCTURE ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE STRUCTURE ITEM</em>'.
	 * @generated
	 */
	CUBE_STRUCTURE_ITEM createCUBE_STRUCTURE_ITEM();

	/**
	 * Returns a new object of class '<em>CUBE TO COMBINATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE TO COMBINATION</em>'.
	 * @generated
	 */
	CUBE_TO_COMBINATION createCUBE_TO_COMBINATION();

	/**
	 * Returns a new object of class '<em>FRAMEWORK</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FRAMEWORK</em>'.
	 * @generated
	 */
	FRAMEWORK createFRAMEWORK();

	/**
	 * Returns a new object of class '<em>FRAMEWORK SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FRAMEWORK SUBDOMAIN</em>'.
	 * @generated
	 */
	FRAMEWORK_SUBDOMAIN createFRAMEWORK_SUBDOMAIN();

	/**
	 * Returns a new object of class '<em>FRAMEWORK VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FRAMEWORK VARIABLE SET</em>'.
	 * @generated
	 */
	FRAMEWORK_VARIABLE_SET createFRAMEWORK_VARIABLE_SET();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Data_definitionPackage getData_definitionPackage();

} //Data_definitionFactory
