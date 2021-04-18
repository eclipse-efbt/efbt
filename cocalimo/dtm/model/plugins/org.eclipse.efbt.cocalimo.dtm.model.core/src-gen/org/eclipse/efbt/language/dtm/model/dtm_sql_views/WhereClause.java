/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BooleanFunction;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Where Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * WhereClause contains a boolean function used in filters and joins. This is equivalent to where clause in  SQL.     
 *     
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.WhereClause#getFunction <em>Function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getWhereClause()
 * @model extendedMetaData="name='WhereClause' kind='elementOnly'"
 * @generated
 */
public interface WhereClause extends EObject
{
	/**
	 * Returns the value of the '<em><b>Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The boolean function
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Function</em>' containment reference.
	 * @see #setFunction(BooleanFunction)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getWhereClause_Function()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='function'"
	 * @generated
	 */
	BooleanFunction getFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.WhereClause#getFunction <em>Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function</em>' containment reference.
	 * @see #getFunction()
	 * @generated
	 */
	void setFunction(BooleanFunction value);

} // WhereClause
