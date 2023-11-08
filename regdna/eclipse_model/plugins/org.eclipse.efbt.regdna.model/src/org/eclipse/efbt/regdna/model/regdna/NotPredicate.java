/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Not Predicate</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.NotPredicate#getOperand <em>Operand</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getNotPredicate()
 * @model
 * @generated
 */
public interface NotPredicate extends Predicate {
	/**
	 * Returns the value of the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operand</em>' containment reference.
	 * @see #setOperand(Predicate)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getNotPredicate_Operand()
	 * @model containment="true"
	 * @generated
	 */
	Predicate getOperand();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.NotPredicate#getOperand <em>Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operand</em>' containment reference.
	 * @see #getOperand()
	 * @generated
	 */
	void setOperand(Predicate value);

} // NotPredicate
