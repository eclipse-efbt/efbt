/**
 */
package advanced_row_transformation_logic;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see advanced_row_transformation_logic.Advanced_row_transformation_logicPackage
 * @generated
 */
public interface Advanced_row_transformation_logicFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Advanced_row_transformation_logicFactory eINSTANCE = advanced_row_transformation_logic.impl.Advanced_row_transformation_logicFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Explode Array Of Structs Row Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Explode Array Of Structs Row Function</em>'.
	 * @generated
	 */
	ExplodeArrayOfStructsRowFunction createExplodeArrayOfStructsRowFunction();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Advanced_row_transformation_logicPackage getAdvanced_row_transformation_logicPackage();

} //Advanced_row_transformation_logicFactory
