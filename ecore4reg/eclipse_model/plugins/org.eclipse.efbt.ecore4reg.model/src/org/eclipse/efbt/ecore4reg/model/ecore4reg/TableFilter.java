/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Table Filter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getPredicate <em>Predicate</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTableFilter()
 * @model
 * @generated
 */
public interface TableFilter extends EObject {
	/**
	 * Returns the value of the '<em><b>Predicate</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Predicate</em>' containment reference.
	 * @see #setPredicate(Predicate)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTableFilter_Predicate()
	 * @model containment="true"
	 * @generated
	 */
	Predicate getPredicate();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getPredicate <em>Predicate</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Predicate</em>' containment reference.
	 * @see #getPredicate()
	 * @generated
	 */
	void setPredicate(Predicate value);

} // TableFilter
