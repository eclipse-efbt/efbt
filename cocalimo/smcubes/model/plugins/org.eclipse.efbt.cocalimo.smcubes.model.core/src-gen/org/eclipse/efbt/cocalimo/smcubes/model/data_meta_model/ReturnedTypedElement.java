/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Returned Typed Element</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getOrdered <em>Ordered</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUnique <em>Unique</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getLowerBound <em>Lower Bound</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUpperBound <em>Upper Bound</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getMany <em>Many</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getRequired <em>Required</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getEntityType <em>Entity Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement()
 * @model abstract="true"
 *        annotation="http://www.eclipse.org/emf/2002/Ecore constraints='ValidLowerBound ValidUpperBound ConsistentBounds ValidType'"
 * @generated
 */
public interface ReturnedTypedElement extends TypedElement {
	/**
	 * Returns the value of the '<em><b>Ordered</b></em>' attribute.
	 * The default value is <code>"true"</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ordered</em>' attribute.
	 * @see #setOrdered(Object)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_Ordered()
	 * @model default="true" dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Boolean"
	 * @generated
	 */
	Object getOrdered();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getOrdered <em>Ordered</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ordered</em>' attribute.
	 * @see #getOrdered()
	 * @generated
	 */
	void setOrdered(Object value);

	/**
	 * Returns the value of the '<em><b>Unique</b></em>' attribute.
	 * The default value is <code>"true"</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Unique</em>' attribute.
	 * @see #setUnique(Object)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_Unique()
	 * @model default="true" dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Boolean"
	 * @generated
	 */
	Object getUnique();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUnique <em>Unique</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Unique</em>' attribute.
	 * @see #getUnique()
	 * @generated
	 */
	void setUnique(Object value);

	/**
	 * Returns the value of the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Lower Bound</em>' attribute.
	 * @see #setLowerBound(Object)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_LowerBound()
	 * @model dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Int"
	 * @generated
	 */
	Object getLowerBound();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getLowerBound <em>Lower Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Lower Bound</em>' attribute.
	 * @see #getLowerBound()
	 * @generated
	 */
	void setLowerBound(Object value);

	/**
	 * Returns the value of the '<em><b>Upper Bound</b></em>' attribute.
	 * The default value is <code>"1"</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Upper Bound</em>' attribute.
	 * @see #setUpperBound(Object)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_UpperBound()
	 * @model default="1" dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Int"
	 * @generated
	 */
	Object getUpperBound();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUpperBound <em>Upper Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Upper Bound</em>' attribute.
	 * @see #getUpperBound()
	 * @generated
	 */
	void setUpperBound(Object value);

	/**
	 * Returns the value of the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_Many()
	 * @model dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Boolean" transient="true" changeable="false" volatile="true" derived="true"
	 * @generated
	 */
	Object getMany();

	/**
	 * Returns the value of the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Required</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_Required()
	 * @model dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Boolean" transient="true" changeable="false" volatile="true" derived="true"
	 * @generated
	 */
	Object getRequired();

	/**
	 * Returns the value of the '<em><b>Entity Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity Type</em>' reference.
	 * @see #setEntityType(Entity)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getReturnedTypedElement_EntityType()
	 * @model unsettable="true" volatile="true" suppressedIsSetVisibility="true" suppressedUnsetVisibility="true"
	 * @generated
	 */
	Entity getEntityType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getEntityType <em>Entity Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity Type</em>' reference.
	 * @see #getEntityType()
	 * @generated
	 */
	void setEntityType(Entity value);

} // ReturnedTypedElement
