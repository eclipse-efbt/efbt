/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage
 * @generated
 */
public interface Efbt_vtl_transformationFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_vtl_transformationFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.impl.Efbt_vtl_transformationFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Transformation Scheme Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Transformation Scheme Module</em>'.
	 * @generated
	 */
	TransformationSchemeModule createTransformationSchemeModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Efbt_vtl_transformationPackage getEfbt_vtl_transformationPackage();

} //Efbt_vtl_transformationFactory
