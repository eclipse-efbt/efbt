/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage
 * @generated
 */
public interface MappingFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	MappingFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>CUBE MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CUBE MAPPING</em>'.
	 * @generated
	 */
	CUBE_MAPPING createCUBE_MAPPING();

	/**
	 * Returns a new object of class '<em>MEMBER MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER MAPPING</em>'.
	 * @generated
	 */
	MEMBER_MAPPING createMEMBER_MAPPING();

	/**
	 * Returns a new object of class '<em>MEMBER MAPPING ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER MAPPING ITEM</em>'.
	 * @generated
	 */
	MEMBER_MAPPING_ITEM createMEMBER_MAPPING_ITEM();

	/**
	 * Returns a new object of class '<em>VARIABLE MAPPING ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE MAPPING ITEM</em>'.
	 * @generated
	 */
	VARIABLE_MAPPING_ITEM createVARIABLE_MAPPING_ITEM();

	/**
	 * Returns a new object of class '<em>VARIABLE MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE MAPPING</em>'.
	 * @generated
	 */
	VARIABLE_MAPPING createVARIABLE_MAPPING();

	/**
	 * Returns a new object of class '<em>MAPPING TO CUBE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MAPPING TO CUBE</em>'.
	 * @generated
	 */
	MAPPING_TO_CUBE createMAPPING_TO_CUBE();

	/**
	 * Returns a new object of class '<em>VARIABLE SET MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE SET MAPPING</em>'.
	 * @generated
	 */
	VARIABLE_SET_MAPPING createVARIABLE_SET_MAPPING();

	/**
	 * Returns a new object of class '<em>MAPPING DEFINITION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MAPPING DEFINITION</em>'.
	 * @generated
	 */
	MAPPING_DEFINITION createMAPPING_DEFINITION();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	MappingPackage getMappingPackage();

} //MappingFactory
