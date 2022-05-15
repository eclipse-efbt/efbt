/**
 */
package org.eclipse.efbt.openregspecs.model.core;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage
 * @generated
 */
public interface CoreFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	CoreFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.core.impl.CoreFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>DOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>DOMAIN</em>'.
	 * @generated
	 */
	DOMAIN createDOMAIN();

	/**
	 * Returns a new object of class '<em>FACET COLLECTION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FACET COLLECTION</em>'.
	 * @generated
	 */
	FACET_COLLECTION createFACET_COLLECTION();

	/**
	 * Returns a new object of class '<em>FACET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FACET ENUMERATION</em>'.
	 * @generated
	 */
	FACET_ENUMERATION createFACET_ENUMERATION();

	/**
	 * Returns a new object of class '<em>facet type</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>facet type</em>'.
	 * @generated
	 */
	facet_type createfacet_type();

	/**
	 * Returns a new object of class '<em>MAINTENANCE AGENCY</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MAINTENANCE AGENCY</em>'.
	 * @generated
	 */
	MAINTENANCE_AGENCY createMAINTENANCE_AGENCY();

	/**
	 * Returns a new object of class '<em>MEMBER</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER</em>'.
	 * @generated
	 */
	MEMBER createMEMBER();

	/**
	 * Returns a new object of class '<em>MEMBER HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER HIERARCHY</em>'.
	 * @generated
	 */
	MEMBER_HIERARCHY createMEMBER_HIERARCHY();

	/**
	 * Returns a new object of class '<em>MEMBER HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER HIERARCHY NODE</em>'.
	 * @generated
	 */
	MEMBER_HIERARCHY_NODE createMEMBER_HIERARCHY_NODE();

	/**
	 * Returns a new object of class '<em>SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SUBDOMAIN</em>'.
	 * @generated
	 */
	SUBDOMAIN createSUBDOMAIN();

	/**
	 * Returns a new object of class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @generated
	 */
	SUBDOMAIN_ENUMERATION createSUBDOMAIN_ENUMERATION();

	/**
	 * Returns a new object of class '<em>VARIABLE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE</em>'.
	 * @generated
	 */
	VARIABLE createVARIABLE();

	/**
	 * Returns a new object of class '<em>VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE SET</em>'.
	 * @generated
	 */
	VARIABLE_SET createVARIABLE_SET();

	/**
	 * Returns a new object of class '<em>VARIABLE SET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE SET ENUMERATION</em>'.
	 * @generated
	 */
	VARIABLE_SET_ENUMERATION createVARIABLE_SET_ENUMERATION();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	CorePackage getCorePackage();

} //CoreFactory
