/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Class</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#isAbstract <em>Abstract</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getESuperTypes <em>ESuper Types</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEStructuralFeatures <em>EStructural Features</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEOperations <em>EOperations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClass()
 * @model
 * @generated
 */
public interface ELClass extends ELClassifier {
	/**
	 * Returns the value of the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Abstract</em>' attribute.
	 * @see #setAbstract(boolean)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClass_Abstract()
	 * @model
	 * @generated
	 */
	boolean isAbstract();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#isAbstract <em>Abstract</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Abstract</em>' attribute.
	 * @see #isAbstract()
	 * @generated
	 */
	void setAbstract(boolean value);

	/**
	 * Returns the value of the '<em><b>ESuper Types</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>ESuper Types</em>' reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClass_ESuperTypes()
	 * @model unsettable="true" suppressedIsSetVisibility="true" suppressedUnsetVisibility="true"
	 * @generated
	 */
	EList<ELClass> getESuperTypes();

	/**
	 * Returns the value of the '<em><b>EStructural Features</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EStructural Features</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClass_EStructuralFeatures()
	 * @model containment="true"
	 * @generated
	 */
	EList<ELStructuralFeature> getEStructuralFeatures();

	/**
	 * Returns the value of the '<em><b>EOperations</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EOperations</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELClass_EOperations()
	 * @model containment="true"
	 * @generated
	 */
	EList<ELOperation> getEOperations();

} // ELClass
