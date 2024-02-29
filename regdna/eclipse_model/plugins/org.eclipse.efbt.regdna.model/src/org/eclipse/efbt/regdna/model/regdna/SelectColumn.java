/**
 */
package org.eclipse.efbt.regdna.model.regdna;

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
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.SelectColumn#getAsAttribute <em>As Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getSelectColumn()
 * @model
 * @generated
 */
public interface SelectColumn extends EObject {
	/**
	 * Returns the value of the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>As Attribute</em>' reference.
	 * @see #setAsAttribute(ELOperation)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getSelectColumn_AsAttribute()
	 * @model
	 * @generated
	 */
	ELOperation getAsAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumn#getAsAttribute <em>As Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>As Attribute</em>' reference.
	 * @see #getAsAttribute()
	 * @generated
	 */
	void setAsAttribute(ELOperation value);

} // SelectColumn
