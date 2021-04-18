/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Group By Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *    This is equivalent to the Group By section  in SQL such as select ccy, country, sum(amount) from trades, group by ccy, country.
 *       The resulting cube structure will be a column for each of the groupBy columns, with one column added per calculated column.
 *       There will likely be considerably less rows in the results of the view, for example if we are grouping on million trades
 *       by currency, there will be on row per unique currency in the results.
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.GroupByClause#getGroupByColumns <em>Group By Columns</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGroupByClause()
 * @model extendedMetaData="name='GroupByClause' kind='empty'"
 * @generated
 */
public interface GroupByClause extends EObject
{
	/**
	 * Returns the value of the '<em><b>Group By Columns</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the columns to group by 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Group By Columns</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getGroupByClause_GroupByColumns()
	 * @model extendedMetaData="kind='attribute' name='groupByColumns'"
	 * @generated
	 */
	EList<VARIABLE> getGroupByColumns();

} // GroupByClause
