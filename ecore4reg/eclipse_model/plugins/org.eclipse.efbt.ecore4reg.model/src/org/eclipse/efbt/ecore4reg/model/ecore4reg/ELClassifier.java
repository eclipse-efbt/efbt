/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Classifier</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier#getEPackage <em>EPackage</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClassifier()
 * @model abstract="true"
 * @generated
 */
public interface ELClassifier extends ELNamedElement {
	/**
	 * Returns the value of the '<em><b>EPackage</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getEClassifiers <em>EClassifiers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EPackage</em>' container reference.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClassifier_EPackage()
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getEClassifiers
	 * @model opposite="eClassifiers" changeable="false"
	 * @generated
	 */
	ELPackage getEPackage();

} // ELClassifier
