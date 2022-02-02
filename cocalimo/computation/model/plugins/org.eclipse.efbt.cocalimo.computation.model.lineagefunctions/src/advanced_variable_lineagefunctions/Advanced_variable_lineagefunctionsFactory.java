/**
 */
package advanced_variable_lineagefunctions;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage
 * @generated
 */
public interface Advanced_variable_lineagefunctionsFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Advanced_variable_lineagefunctionsFactory eINSTANCE = advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Get Struct Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Get Struct Column Function</em>'.
	 * @generated
	 */
	GetStructColumnFunction createGetStructColumnFunction();

	/**
	 * Returns a new object of class '<em>Resolved Struct Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Resolved Struct Column Parameter</em>'.
	 * @generated
	 */
	ResolvedStructColumnParameter createResolvedStructColumnParameter();

	/**
	 * Returns a new object of class '<em>Speculative Struct Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Speculative Struct Column Parameter</em>'.
	 * @generated
	 */
	SpeculativeStructColumnParameter createSpeculativeStructColumnParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Advanced_variable_lineagefunctionsPackage getAdvanced_variable_lineagefunctionsPackage();

} //Advanced_variable_lineagefunctionsFactory
