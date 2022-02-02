/**
 */
package entity_functions;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see entity_functions.Entity_functionsPackage
 * @generated
 */
public interface Entity_functionsFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Entity_functionsFactory eINSTANCE = entity_functions.impl.Entity_functionsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Member Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Parameter</em>'.
	 * @generated
	 */
	MemberParameter createMemberParameter();

	/**
	 * Returns a new object of class '<em>Value Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Value Parameter</em>'.
	 * @generated
	 */
	ValueParameter createValueParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Entity_functionsPackage getEntity_functionsPackage();

} //Entity_functionsFactory
