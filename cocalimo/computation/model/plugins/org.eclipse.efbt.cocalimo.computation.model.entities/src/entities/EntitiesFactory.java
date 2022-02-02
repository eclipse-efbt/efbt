/**
 */
package entities;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see entities.EntitiesPackage
 * @generated
 */
public interface EntitiesFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	EntitiesFactory eINSTANCE = entities.impl.EntitiesFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Base Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Entity</em>'.
	 * @generated
	 */
	BaseEntity createBaseEntity();

	/**
	 * Returns a new object of class '<em>Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Entity</em>'.
	 * @generated
	 */
	DerivedEntity createDerivedEntity();

	/**
	 * Returns a new object of class '<em>Generated Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generated Entity</em>'.
	 * @generated
	 */
	GeneratedEntity createGeneratedEntity();

	/**
	 * Returns a new object of class '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell</em>'.
	 * @generated
	 */
	Cell createCell();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	EntitiesPackage getEntitiesPackage();

} //EntitiesFactory
