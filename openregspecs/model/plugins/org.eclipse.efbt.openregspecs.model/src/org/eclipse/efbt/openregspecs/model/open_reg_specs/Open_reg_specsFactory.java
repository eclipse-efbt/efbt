/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage
 * @generated
 */
public interface Open_reg_specsFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Open_reg_specsFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.Open_reg_specsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Open Reg Specs</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Open Reg Specs</em>'.
	 * @generated
	 */
	OpenRegSpecs createOpenRegSpecs();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Open_reg_specsPackage getOpen_reg_specsPackage();

} //Open_reg_specsFactory
