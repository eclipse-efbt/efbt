/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Filter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOutputLayer <em>Output Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOperation <em>Operation</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Filter#getMember <em>Member</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getFilter()
 * @model
 * @generated
 */
public interface Filter extends EObject {
	/**
	 * Returns the value of the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Layer</em>' reference.
	 * @see #setOutputLayer(ELClass)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getFilter_OutputLayer()
	 * @model
	 * @generated
	 */
	ELClass getOutputLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOutputLayer <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Layer</em>' reference.
	 * @see #getOutputLayer()
	 * @generated
	 */
	void setOutputLayer(ELClass value);

	/**
	 * Returns the value of the '<em><b>Operation</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operation</em>' reference.
	 * @see #setOperation(ELOperation)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getFilter_Operation()
	 * @model
	 * @generated
	 */
	ELOperation getOperation();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOperation <em>Operation</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operation</em>' reference.
	 * @see #getOperation()
	 * @generated
	 */
	void setOperation(ELOperation value);

	/**
	 * Returns the value of the '<em><b>Member</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member</em>' reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getFilter_Member()
	 * @model
	 * @generated
	 */
	EList<ELEnumLiteral> getMember();

} // Filter
