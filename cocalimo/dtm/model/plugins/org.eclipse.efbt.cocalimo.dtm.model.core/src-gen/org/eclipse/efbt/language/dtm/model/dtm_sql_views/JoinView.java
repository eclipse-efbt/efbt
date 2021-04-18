/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Join View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView which represents a join of  rows of  two source cubes of the associated table,  according to a join condition.
 * This is equivalent to joining 2 tables using  SQL 
 * The resulting cube structure will be a column for each of the  columns in the 2 source cubes  unless we specify that we want to ignore columns or rename columns.
 *     
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.JoinView#getWhereClause <em>Where Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.JoinView#getRenames <em>Renames</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.JoinView#getIgnores <em>Ignores</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getJoinView()
 * @model extendedMetaData="name='JoinView' kind='elementOnly'"
 * @generated
 */
public interface JoinView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A WhereClause clause representing the join condition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Where Clause</em>' containment reference.
	 * @see #setWhereClause(WhereClause)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getJoinView_WhereClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='whereClause'"
	 * @generated
	 */
	WhereClause getWhereClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.JoinView#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(WhereClause value);

	/**
	 * Returns the value of the '<em><b>Renames</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.RenameAs}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A rename for each column we wish to rename
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Renames</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getJoinView_Renames()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='renames'"
	 * @generated
	 */
	EList<RenameAs> getRenames();

	/**
	 * Returns the value of the '<em><b>Ignores</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.IgnoreColumn}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * an IgnoreColumn for each column we want to ignore
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Ignores</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getJoinView_Ignores()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='ignores'"
	 * @generated
	 */
	EList<IgnoreColumn> getIgnores();

} // JoinView
