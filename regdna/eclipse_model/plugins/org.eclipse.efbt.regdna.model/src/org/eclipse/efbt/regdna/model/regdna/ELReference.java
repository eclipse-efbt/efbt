/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Reference</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELReference#isContainment <em>Containment</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELReference#getEReferenceType <em>EReference Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELReference()
 * @model
 * @generated
 */
public interface ELReference extends ELStructuralFeature {
	/**
	 * Returns the value of the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Containment</em>' attribute.
	 * @see #setContainment(boolean)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELReference_Containment()
	 * @model
	 * @generated
	 */
	boolean isContainment();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELReference#isContainment <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Containment</em>' attribute.
	 * @see #isContainment()
	 * @generated
	 */
	void setContainment(boolean value);

	/**
	 * Returns the value of the '<em><b>EReference Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EReference Type</em>' reference.
	 * @see #setEReferenceType(ELClassifier)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELReference_EReferenceType()
	 * @model
	 * @generated
	 */
	ELClassifier getEReferenceType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELReference#getEReferenceType <em>EReference Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>EReference Type</em>' reference.
	 * @see #getEReferenceType()
	 * @generated
	 */
	void setEReferenceType(ELClassifier value);

} // ELReference
