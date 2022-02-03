/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Where Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.WhereClause#getWhere_function <em>Where function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getWhereClause()
 * @model
 * @generated
 */
public interface WhereClause extends EObject {
	/**
	 * Returns the value of the '<em><b>Where function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Where function</em>' containment reference.
	 * @see #setWhere_function(BooleanFunction)
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getWhereClause_Where_function()
	 * @model containment="true"
	 * @generated
	 */
	BooleanFunction getWhere_function();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.WhereClause#getWhere_function <em>Where function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where function</em>' containment reference.
	 * @see #getWhere_function()
	 * @generated
	 */
	void setWhere_function(BooleanFunction value);

} // WhereClause
