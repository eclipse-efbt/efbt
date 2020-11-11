/**
 */
package cubes;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see cubes.CubesPackage
 * @generated
 */
public interface CubesFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	CubesFactory eINSTANCE = cubes.impl.CubesFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Base Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Cube</em>'.
	 * @generated
	 */
	BaseCube createBaseCube();

	/**
	 * Returns a new object of class '<em>Derived Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Cube</em>'.
	 * @generated
	 */
	DerivedCube createDerivedCube();

	/**
	 * Returns a new object of class '<em>Free Bird Tools Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Free Bird Tools Cube</em>'.
	 * @generated
	 */
	FreeBirdToolsCube createFreeBirdToolsCube();

	/**
	 * Returns a new object of class '<em>Free Bird Tools Cube Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Free Bird Tools Cube Module</em>'.
	 * @generated
	 */
	FreeBirdToolsCubeModule createFreeBirdToolsCubeModule();

	/**
	 * Returns a new object of class '<em>Target Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Target Cube</em>'.
	 * @generated
	 */
	TargetCube createTargetCube();

	/**
	 * Returns a new object of class '<em>Base Delta Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Delta Cube</em>'.
	 * @generated
	 */
	BaseDeltaCube createBaseDeltaCube();

	/**
	 * Returns a new object of class '<em>Base View Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base View Cube</em>'.
	 * @generated
	 */
	BaseViewCube createBaseViewCube();

	/**
	 * Returns a new object of class '<em>Delta Accumulation Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Delta Accumulation Cube</em>'.
	 * @generated
	 */
	DeltaAccumulationCube createDeltaAccumulationCube();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	CubesPackage getCubesPackage();

} //CubesFactory
