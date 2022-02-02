/**
 */
package sql_lite;

import functions.BooleanFunction;

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
 *   <li>{@link sql_lite.WhereClause#getWhere_function <em>Where function</em>}</li>
 * </ul>
 *
 * @see sql_lite.Sql_litePackage#getWhereClause()
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
	 * @see sql_lite.Sql_litePackage#getWhereClause_Where_function()
	 * @model containment="true"
	 * @generated
	 */
	BooleanFunction getWhere_function();

	/**
	 * Sets the value of the '{@link sql_lite.WhereClause#getWhere_function <em>Where function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where function</em>' containment reference.
	 * @see #getWhere_function()
	 * @generated
	 */
	void setWhere_function(BooleanFunction value);

} // WhereClause
