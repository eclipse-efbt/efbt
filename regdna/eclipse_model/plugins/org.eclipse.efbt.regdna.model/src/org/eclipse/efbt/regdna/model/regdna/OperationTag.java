/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Operation Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.OperationTag#getOperation <em>Operation</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getOperationTag()
 * @model
 * @generated
 */
public interface OperationTag extends Tag {
	/**
	 * Returns the value of the '<em><b>Operation</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operation</em>' reference.
	 * @see #setOperation(ELOperation)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getOperationTag_Operation()
	 * @model
	 * @generated
	 */
	ELOperation getOperation();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.OperationTag#getOperation <em>Operation</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operation</em>' reference.
	 * @see #getOperation()
	 * @generated
	 */
	void setOperation(ELOperation value);

} // OperationTag
