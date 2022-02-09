/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getIsPK <em>Is PK</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getAttribute()
 * @model
 * @generated
 */
public interface Attribute extends StructuralFeature {
	/**
	 * Returns the value of the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is PK</em>' attribute.
	 * @see #setIsPK(Object)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getAttribute_IsPK()
	 * @model dataType="org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Boolean"
	 * @generated
	 */
	Object getIsPK();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getIsPK <em>Is PK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is PK</em>' attribute.
	 * @see #getIsPK()
	 * @generated
	 */
	void setIsPK(Object value);

	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getAttribute_Variable()
	 * @model
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

} // Attribute
