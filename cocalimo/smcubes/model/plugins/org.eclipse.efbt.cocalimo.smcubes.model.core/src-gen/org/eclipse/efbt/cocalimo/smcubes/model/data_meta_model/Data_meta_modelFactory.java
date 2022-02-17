/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage
 * @generated
 */
public interface Data_meta_modelFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_meta_modelFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute</em>'.
	 * @generated
	 */
	Attribute createAttribute();

	/**
	 * Returns a new object of class '<em>Foreign Key Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Foreign Key Attribute</em>'.
	 * @generated
	 */
	ForeignKeyAttribute createForeignKeyAttribute();

	/**
	 * Returns a new object of class '<em>Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Entity Module</em>'.
	 * @generated
	 */
	EntityModule createEntityModule();

	/**
	 * Returns a new object of class '<em>Generated Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generated Entity</em>'.
	 * @generated
	 */
	GeneratedEntity createGeneratedEntity();

	/**
	 * Returns a new object of class '<em>Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Entity</em>'.
	 * @generated
	 */
	DerivedEntity createDerivedEntity();

	/**
	 * Returns a new object of class '<em>Basic Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Basic Entity</em>'.
	 * @generated
	 */
	BasicEntity createBasicEntity();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Data_meta_modelPackage getData_meta_modelPackage();

} //Data_meta_modelFactory
