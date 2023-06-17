/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL Scheme</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getExpressions <em>Expressions</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getScheme_id <em>Scheme id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLScheme()
 * @model
 * @generated
 */
public interface VTLScheme extends EObject {
	/**
	 * Returns the value of the '<em><b>Expressions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expressions</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLScheme_Expressions()
	 * @model containment="true"
	 * @generated
	 */
	EList<VTLTransformation> getExpressions();

	/**
	 * Returns the value of the '<em><b>Scheme id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scheme id</em>' attribute.
	 * @see #setScheme_id(String)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLScheme_Scheme_id()
	 * @model
	 * @generated
	 */
	String getScheme_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getScheme_id <em>Scheme id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scheme id</em>' attribute.
	 * @see #getScheme_id()
	 * @generated
	 */
	void setScheme_id(String value);

} // VTLScheme
