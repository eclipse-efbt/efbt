/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Typed Element</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getEType <em>EType</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getUpperBound <em>Upper Bound</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getLowerBound <em>Lower Bound</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELTypedElement()
 * @model abstract="true"
 * @generated
 */
public interface ELTypedElement extends ELNamedElement {
	/**
	 * Returns the value of the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EType</em>' reference.
	 * @see #setEType(ELClassifier)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELTypedElement_EType()
	 * @model unsettable="true" suppressedIsSetVisibility="true" suppressedUnsetVisibility="true"
	 * @generated
	 */
	ELClassifier getEType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getEType <em>EType</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>EType</em>' reference.
	 * @see #getEType()
	 * @generated
	 */
	void setEType(ELClassifier value);

	/**
	 * Returns the value of the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Upper Bound</em>' attribute.
	 * @see #setUpperBound(int)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELTypedElement_UpperBound()
	 * @model
	 * @generated
	 */
	int getUpperBound();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getUpperBound <em>Upper Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Upper Bound</em>' attribute.
	 * @see #getUpperBound()
	 * @generated
	 */
	void setUpperBound(int value);

	/**
	 * Returns the value of the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Lower Bound</em>' attribute.
	 * @see #setLowerBound(int)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELTypedElement_LowerBound()
	 * @model
	 * @generated
	 */
	int getLowerBound();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getLowerBound <em>Lower Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Lower Bound</em>' attribute.
	 * @see #getLowerBound()
	 * @generated
	 */
	void setLowerBound(int value);

} // ELTypedElement
