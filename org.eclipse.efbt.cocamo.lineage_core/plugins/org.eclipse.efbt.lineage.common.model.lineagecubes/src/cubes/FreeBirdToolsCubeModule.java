/**
 */
package cubes;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Free Bird Tools Cube Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  A Module containing a list of FreeBirdToolsCubes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link cubes.FreeBirdToolsCubeModule#getCubes <em>Cubes</em>}</li>
 * </ul>
 *
 * @see cubes.CubesPackage#getFreeBirdToolsCubeModule()
 * @model extendedMetaData="name='FreeBirdToolsCubeModule' kind='elementOnly'"
 * @generated
 */
public interface FreeBirdToolsCubeModule extends org.eclipse.efbt.cocamo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Cubes</b></em>' containment reference list.
	 * The list contents are of type {@link cubes.FreeBirdToolsCube}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 *  The list of FreeBirdToolsCubes
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cubes</em>' containment reference list.
	 * @see cubes.CubesPackage#getFreeBirdToolsCubeModule_Cubes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubes'"
	 * @generated
	 */
	EList<FreeBirdToolsCube> getCubes();

} // FreeBirdToolsCubeModule
