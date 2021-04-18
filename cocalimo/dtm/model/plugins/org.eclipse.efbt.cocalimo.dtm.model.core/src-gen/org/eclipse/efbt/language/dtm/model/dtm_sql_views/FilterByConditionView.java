/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Filter By Condition View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView which represents a Copy of all rows  of the single source cube of the associated cube IF they meet a filter condition.
 *       The resulting cube structure will be the same as that source cube.
 *       The number of rows in the view will be the number of rows in the source table that meet the supplied condition.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByConditionView#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByConditionView()
 * @model extendedMetaData="name='FilterByConditionView' kind='elementOnly'"
 * @generated
 */
public interface FilterByConditionView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A WhereClause clause representing the filter condition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Where Clause</em>' containment reference.
	 * @see #setWhereClause(WhereClause)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByConditionView_WhereClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='whereClause'"
	 * @generated
	 */
	WhereClause getWhereClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByConditionView#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(WhereClause value);

} // FilterByConditionView
