/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Package</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getEClassifiers <em>EClassifiers</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsURI <em>Ns URI</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsPrefix <em>Ns Prefix</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPackage()
 * @model
 * @generated
 */
public interface ELPackage extends org.eclipse.efbt.regdna.model.regdna.Module {
	/**
	 * Returns the value of the '<em><b>EClassifiers</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ELClassifier}.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.regdna.model.regdna.ELClassifier#getEPackage <em>EPackage</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EClassifiers</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPackage_EClassifiers()
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClassifier#getEPackage
	 * @model opposite="ePackage" containment="true"
	 * @generated
	 */
	EList<ELClassifier> getEClassifiers();

	/**
	 * Returns the value of the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ns URI</em>' attribute.
	 * @see #setNsURI(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPackage_NsURI()
	 * @model
	 * @generated
	 */
	String getNsURI();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsURI <em>Ns URI</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ns URI</em>' attribute.
	 * @see #getNsURI()
	 * @generated
	 */
	void setNsURI(String value);

	/**
	 * Returns the value of the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ns Prefix</em>' attribute.
	 * @see #setNsPrefix(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPackage_NsPrefix()
	 * @model
	 * @generated
	 */
	String getNsPrefix();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsPrefix <em>Ns Prefix</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ns Prefix</em>' attribute.
	 * @see #getNsPrefix()
	 * @generated
	 */
	void setNsPrefix(String value);

} // ELPackage
