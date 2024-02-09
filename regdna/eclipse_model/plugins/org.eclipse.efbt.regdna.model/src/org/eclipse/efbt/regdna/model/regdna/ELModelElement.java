/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

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
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELModelElement#getEAnnotations <em>EAnnotations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELModelElement()
 * @model abstract="true"
 * @generated
 */
public interface ELModelElement extends EObject {
	/**
	 * Returns the value of the '<em><b>EAnnotations</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ELAnnotation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EAnnotations</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELModelElement_EAnnotations()
	 * @model containment="true"
	 * @generated
	 */
	EList<ELAnnotation> getEAnnotations();

} // ELModelElement
