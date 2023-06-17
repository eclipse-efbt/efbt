/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Model Element</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement#getEAnnotations <em>EAnnotations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELModelElement()
 * @model abstract="true"
 * @generated
 */
public interface ELModelElement extends EObject {
	/**
	 * Returns the value of the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EAnnotations</em>' containment reference.
	 * @see #setEAnnotations(ELAnnotation)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELModelElement_EAnnotations()
	 * @model containment="true"
	 * @generated
	 */
	ELAnnotation getEAnnotations();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement#getEAnnotations <em>EAnnotations</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>EAnnotations</em>' containment reference.
	 * @see #getEAnnotations()
	 * @generated
	 */
	void setEAnnotations(ELAnnotation value);

} // ELModelElement
