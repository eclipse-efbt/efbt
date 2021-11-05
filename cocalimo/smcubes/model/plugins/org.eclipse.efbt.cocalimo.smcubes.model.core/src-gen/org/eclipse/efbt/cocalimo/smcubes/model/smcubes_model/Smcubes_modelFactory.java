/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage
 * @generated
 */
public interface Smcubes_modelFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Smcubes_modelFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Smcubes Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Smcubes Model</em>'.
	 * @generated
	 */
	SmcubesModel createSmcubesModel();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Smcubes_modelPackage getSmcubes_modelPackage();

} //Smcubes_modelFactory
