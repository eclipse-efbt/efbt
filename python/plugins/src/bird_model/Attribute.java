/**
 */
package bird_model;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.Attribute#isIsPK <em>Is PK</em>}</li>
 *   <li>{@link bird_model.Attribute#getVariable <em>Variable</em>}</li>
 *   <li>{@link bird_model.Attribute#isOrdered <em>Ordered</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getAttribute()
 * @model
 * @generated
 */
public interface Attribute extends StructuralFeature {
	/**
	 * Returns the value of the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is PK</em>' attribute.
	 * @see #setIsPK(boolean)
	 * @see bird_model.Bird_modelPackage#getAttribute_IsPK()
	 * @model
	 * @generated
	 */
	boolean isIsPK();

	/**
	 * Sets the value of the '{@link bird_model.Attribute#isIsPK <em>Is PK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is PK</em>' attribute.
	 * @see #isIsPK()
	 * @generated
	 */
	void setIsPK(boolean value);

	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VARIABLE)
	 * @see bird_model.Bird_modelPackage#getAttribute_Variable()
	 * @model
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link bird_model.Attribute#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ordered</em>' attribute.
	 * @see #setOrdered(boolean)
	 * @see bird_model.Bird_modelPackage#getAttribute_Ordered()
	 * @model
	 * @generated
	 */
	boolean isOrdered();

	/**
	 * Sets the value of the '{@link bird_model.Attribute#isOrdered <em>Ordered</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ordered</em>' attribute.
	 * @see #isOrdered()
	 * @generated
	 */
	void setOrdered(boolean value);

} // Attribute
