/**
 */
package bird_model;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Relationship Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.RelationshipAttribute#getEntity <em>Entity</em>}</li>
 *   <li>{@link bird_model.RelationshipAttribute#isContainment <em>Containment</em>}</li>
 *   <li>{@link bird_model.RelationshipAttribute#isMandatory <em>Mandatory</em>}</li>
 *   <li>{@link bird_model.RelationshipAttribute#isDominant <em>Dominant</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getRelationshipAttribute()
 * @model
 * @generated
 */
public interface RelationshipAttribute extends Attribute {
	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(Entity)
	 * @see bird_model.Bird_modelPackage#getRelationshipAttribute_Entity()
	 * @model
	 * @generated
	 */
	Entity getEntity();

	/**
	 * Sets the value of the '{@link bird_model.RelationshipAttribute#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(Entity value);

	/**
	 * Returns the value of the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Containment</em>' attribute.
	 * @see #setContainment(boolean)
	 * @see bird_model.Bird_modelPackage#getRelationshipAttribute_Containment()
	 * @model
	 * @generated
	 */
	boolean isContainment();

	/**
	 * Sets the value of the '{@link bird_model.RelationshipAttribute#isContainment <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Containment</em>' attribute.
	 * @see #isContainment()
	 * @generated
	 */
	void setContainment(boolean value);

	/**
	 * Returns the value of the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mandatory</em>' attribute.
	 * @see #setMandatory(boolean)
	 * @see bird_model.Bird_modelPackage#getRelationshipAttribute_Mandatory()
	 * @model
	 * @generated
	 */
	boolean isMandatory();

	/**
	 * Sets the value of the '{@link bird_model.RelationshipAttribute#isMandatory <em>Mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Mandatory</em>' attribute.
	 * @see #isMandatory()
	 * @generated
	 */
	void setMandatory(boolean value);

	/**
	 * Returns the value of the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Dominant</em>' attribute.
	 * @see #setDominant(boolean)
	 * @see bird_model.Bird_modelPackage#getRelationshipAttribute_Dominant()
	 * @model
	 * @generated
	 */
	boolean isDominant();

	/**
	 * Sets the value of the '{@link bird_model.RelationshipAttribute#isDominant <em>Dominant</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Dominant</em>' attribute.
	 * @see #isDominant()
	 * @generated
	 */
	void setDominant(boolean value);

} // RelationshipAttribute
