/**
 */
package cube_transformation_logic;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see cube_transformation_logic.Cube_transformation_logicPackage
 * @generated
 */
public interface Cube_transformation_logicFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cube_transformation_logicFactory eINSTANCE = cube_transformation_logic.impl.Cube_transformation_logicFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Cube Transformation Logic</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Transformation Logic</em>'.
	 * @generated
	 */
	CubeTransformationLogic createCubeTransformationLogic();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Cube_transformation_logicPackage getCube_transformation_logicPackage();

} //Cube_transformation_logicFactory
