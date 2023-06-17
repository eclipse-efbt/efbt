/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#isID <em>ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#getEAttributeType <em>EAttribute Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELAttribute()
 * @model
 * @generated
 */
public interface ELAttribute extends ELStructuralFeature {
	/**
	 * Returns the value of the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>ID</em>' attribute.
	 * @see #setID(boolean)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELAttribute_ID()
	 * @model
	 * @generated
	 */
	boolean isID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#isID <em>ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>ID</em>' attribute.
	 * @see #isID()
	 * @generated
	 */
	void setID(boolean value);

	/**
	 * Returns the value of the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EAttribute Type</em>' reference.
	 * @see #setEAttributeType(ELDataType)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELAttribute_EAttributeType()
	 * @model
	 * @generated
	 */
	ELDataType getEAttributeType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#getEAttributeType <em>EAttribute Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>EAttribute Type</em>' reference.
	 * @see #getEAttributeType()
	 * @generated
	 */
	void setEAttributeType(ELDataType value);

} // ELAttribute
