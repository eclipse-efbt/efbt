/**
 */
package org.eclipse.efbt.openregspecs.model.data_meta_model;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isIsPK <em>Is PK</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#getConcept <em>Concept</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isOrdered <em>Ordered</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage#getAttribute()
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
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage#getAttribute_IsPK()
	 * @model
	 * @generated
	 */
	boolean isIsPK();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isIsPK <em>Is PK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is PK</em>' attribute.
	 * @see #isIsPK()
	 * @generated
	 */
	void setIsPK(boolean value);

	/**
	 * Returns the value of the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Concept</em>' reference.
	 * @see #setConcept(Concept)
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage#getAttribute_Concept()
	 * @model
	 * @generated
	 */
	Concept getConcept();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#getConcept <em>Concept</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Concept</em>' reference.
	 * @see #getConcept()
	 * @generated
	 */
	void setConcept(Concept value);

	/**
	 * Returns the value of the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ordered</em>' attribute.
	 * @see #setOrdered(boolean)
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage#getAttribute_Ordered()
	 * @model
	 * @generated
	 */
	boolean isOrdered();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isOrdered <em>Ordered</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ordered</em>' attribute.
	 * @see #isOrdered()
	 * @generated
	 */
	void setOrdered(boolean value);

} // Attribute
