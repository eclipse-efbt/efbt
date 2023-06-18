/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Derived Column As</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectDerivedColumnAs#getAttribute <em>Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getSelectDerivedColumnAs()
 * @model
 * @generated
 */
public interface SelectDerivedColumnAs extends SelectColumn {
	/**
	 * Returns the value of the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute</em>' reference.
	 * @see #setAttribute(ELOperation)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getSelectDerivedColumnAs_Attribute()
	 * @model
	 * @generated
	 */
	ELOperation getAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectDerivedColumnAs#getAttribute <em>Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute</em>' reference.
	 * @see #getAttribute()
	 * @generated
	 */
	void setAttribute(ELOperation value);

} // SelectDerivedColumnAs
