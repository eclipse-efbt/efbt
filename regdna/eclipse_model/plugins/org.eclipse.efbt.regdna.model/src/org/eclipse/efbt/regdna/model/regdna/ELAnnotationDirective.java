/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Annotation Directive</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective#getModule <em>Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective#getSourceURI <em>Source URI</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELAnnotationDirective()
 * @model
 * @generated
 */
public interface ELAnnotationDirective extends ELNamedElement {
	/**
	 * Returns the value of the '<em><b>Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Module</em>' reference.
	 * @see #setModule(org.eclipse.efbt.regdna.model.regdna.Module)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELAnnotationDirective_Module()
	 * @model
	 * @generated
	 */
	org.eclipse.efbt.regdna.model.regdna.Module getModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective#getModule <em>Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Module</em>' reference.
	 * @see #getModule()
	 * @generated
	 */
	void setModule(org.eclipse.efbt.regdna.model.regdna.Module value);

	/**
	 * Returns the value of the '<em><b>Source URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source URI</em>' attribute.
	 * @see #setSourceURI(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELAnnotationDirective_SourceURI()
	 * @model
	 * @generated
	 */
	String getSourceURI();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective#getSourceURI <em>Source URI</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source URI</em>' attribute.
	 * @see #getSourceURI()
	 * @generated
	 */
	void setSourceURI(String value);

} // ELAnnotationDirective
