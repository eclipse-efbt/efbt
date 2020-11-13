/**
 */
package cubes;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Free Bird Tools Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  A cube of the FreeBIRDTools platform
 * The FreeBirdToolsCube class is a lightweight class which effectively just has a name, which allows us to define a very high level outline of the relationships between cubes.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link cubes.FreeBirdToolsCube#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see cubes.CubesPackage#getFreeBirdToolsCube()
 * @model extendedMetaData="name='FreeBirdToolsCube' kind='empty'"
 * @generated
 */
public interface FreeBirdToolsCube extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see cubes.CubesPackage#getFreeBirdToolsCube_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link cubes.FreeBirdToolsCube#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // FreeBirdToolsCube
