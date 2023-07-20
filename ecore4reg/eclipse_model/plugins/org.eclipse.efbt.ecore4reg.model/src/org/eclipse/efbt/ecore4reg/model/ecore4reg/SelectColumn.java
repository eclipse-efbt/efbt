/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn#getAsAttribute <em>As Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getSelectColumn()
 * @model
 * @generated
 */
public interface SelectColumn extends EObject {
	/**
	 * Returns the value of the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>As Attribute</em>' reference.
	 * @see #setAsAttribute(ELPublicOperation)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getSelectColumn_AsAttribute()
	 * @model
	 * @generated
	 */
	ELPublicOperation getAsAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn#getAsAttribute <em>As Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>As Attribute</em>' reference.
	 * @see #getAsAttribute()
	 * @generated
	 */
	void setAsAttribute(ELPublicOperation value);

} // SelectColumn
