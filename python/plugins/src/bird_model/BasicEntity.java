/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.BasicEntity#getAttributes <em>Attributes</em>}</li>
 *   <li>{@link bird_model.BasicEntity#getSuperClass <em>Super Class</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getBasicEntity()
 * @model
 * @generated
 */
public interface BasicEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>Attributes</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.Attribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attributes</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBasicEntity_Attributes()
	 * @model containment="true"
	 * @generated
	 */
	EList<Attribute> getAttributes();

	/**
	 * Returns the value of the '<em><b>Super Class</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Class</em>' reference.
	 * @see #setSuperClass(BasicEntity)
	 * @see bird_model.Bird_modelPackage#getBasicEntity_SuperClass()
	 * @model
	 * @generated
	 */
	BasicEntity getSuperClass();

	/**
	 * Sets the value of the '{@link bird_model.BasicEntity#getSuperClass <em>Super Class</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Super Class</em>' reference.
	 * @see #getSuperClass()
	 * @generated
	 */
	void setSuperClass(BasicEntity value);

} // BasicEntity
