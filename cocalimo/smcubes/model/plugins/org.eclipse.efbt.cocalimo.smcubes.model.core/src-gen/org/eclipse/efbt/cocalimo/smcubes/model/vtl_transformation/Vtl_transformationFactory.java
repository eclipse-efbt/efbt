/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage
 * @generated
 */
public interface Vtl_transformationFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Vtl_transformationFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>TRANSFORMATION SCHEME</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>TRANSFORMATION SCHEME</em>'.
	 * @generated
	 */
	TRANSFORMATION_SCHEME createTRANSFORMATION_SCHEME();

	/**
	 * Returns a new object of class '<em>TRANSFORMATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>TRANSFORMATION</em>'.
	 * @generated
	 */
	TRANSFORMATION createTRANSFORMATION();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Vtl_transformationPackage getVtl_transformationPackage();

} //Vtl_transformationFactory
