/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generic View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView which represents a custom transformation.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getSelectClause <em>Select Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getWhereClause <em>Where Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getGroupByClause <em>Group By Clause</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGenericView()
 * @model extendedMetaData="name='GenericView' kind='elementOnly'"
 * @generated
 */
public interface GenericView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Select Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A custom select clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Select Clause</em>' containment reference.
	 * @see #setSelectClause(SelectClause)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGenericView_SelectClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='selectClause'"
	 * @generated
	 */
	SelectClause getSelectClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getSelectClause <em>Select Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Select Clause</em>' containment reference.
	 * @see #getSelectClause()
	 * @generated
	 */
	void setSelectClause(SelectClause value);

	/**
	 * Returns the value of the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A custom where clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Where Clause</em>' containment reference.
	 * @see #setWhereClause(WhereClause)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGenericView_WhereClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='whereClause'"
	 * @generated
	 */
	WhereClause getWhereClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(WhereClause value);

	/**
	 * Returns the value of the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A custom groupby clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Group By Clause</em>' containment reference.
	 * @see #setGroupByClause(GroupByClause)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGenericView_GroupByClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='groupByClause'"
	 * @generated
	 */
	GroupByClause getGroupByClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView#getGroupByClause <em>Group By Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Group By Clause</em>' containment reference.
	 * @see #getGroupByClause()
	 * @generated
	 */
	void setGroupByClause(GroupByClause value);

} // GenericView
