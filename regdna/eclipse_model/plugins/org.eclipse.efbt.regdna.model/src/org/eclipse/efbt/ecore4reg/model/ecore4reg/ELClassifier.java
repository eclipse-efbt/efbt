/**
 */
package org.eclipse.efbt.regdna.model.regdna;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Classifier</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELClassifier#getEPackage <em>EPackage</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELClassifier()
 * @model abstract="true"
 * @generated
 */
public interface ELClassifier extends ELNamedElement {
	/**
	 * Returns the value of the '<em><b>EPackage</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getEClassifiers <em>EClassifiers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EPackage</em>' container reference.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELClassifier_EPackage()
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPackage#getEClassifiers
	 * @model opposite="eClassifiers" changeable="false"
	 * @generated
	 */
	ELPackage getEPackage();

} // ELClassifier
