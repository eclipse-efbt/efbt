/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute Predicate</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getAttribute1 <em>Attribute1</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getComparitor <em>Comparitor</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getMember <em>Member</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getAttributePredicate()
 * @model
 * @generated
 */
public interface AttributePredicate extends Predicate {
	/**
	 * Returns the value of the '<em><b>Attribute1</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute1</em>' reference.
	 * @see #setAttribute1(ELStructuralFeature)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getAttributePredicate_Attribute1()
	 * @model
	 * @generated
	 */
	ELStructuralFeature getAttribute1();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getAttribute1 <em>Attribute1</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute1</em>' reference.
	 * @see #getAttribute1()
	 * @generated
	 */
	void setAttribute1(ELStructuralFeature value);

	/**
	 * Returns the value of the '<em><b>Comparitor</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.regdna.model.regdna.Comparitor}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comparitor</em>' attribute.
	 * @see org.eclipse.efbt.regdna.model.regdna.Comparitor
	 * @see #setComparitor(Comparitor)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getAttributePredicate_Comparitor()
	 * @model
	 * @generated
	 */
	Comparitor getComparitor();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getComparitor <em>Comparitor</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Comparitor</em>' attribute.
	 * @see org.eclipse.efbt.regdna.model.regdna.Comparitor
	 * @see #getComparitor()
	 * @generated
	 */
	void setComparitor(Comparitor value);

	/**
	 * Returns the value of the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member</em>' reference.
	 * @see #setMember(ELEnumLiteral)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getAttributePredicate_Member()
	 * @model
	 * @generated
	 */
	ELEnumLiteral getMember();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getMember <em>Member</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member</em>' reference.
	 * @see #getMember()
	 * @generated
	 */
	void setMember(ELEnumLiteral value);

	/**
	 * Returns the value of the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Value</em>' attribute.
	 * @see #setValue(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getAttributePredicate_Value()
	 * @model
	 * @generated
	 */
	String getValue();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getValue <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' attribute.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(String value);

} // AttributePredicate
