/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aggregate Enrichment View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView which represents a grouping of  rows of the single source table of the associated table, 
 *       according to a set of Columns, and creates extra column(s) which runs a specified  aggregation  function over the group.
 *       This is equivalent to the GroupBy commands in SQL such as select ccy, country, sum(amount) from trades, groupby ccy, country.
 *       The resulting table structure will be a column for each of the groupBy columns, with one column added per calculated column.
 *       There will likely be considerably less rows in the results of the view, for example if we are grouping on million trades
 *       by currency, there will be on row per unique currency in the results.
 *       All SQLViews are associated with a DerivedCube, the source Cubes for the view are defined by the source Cubes of the associated Cube.
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getGroupByClause <em>Group By Clause</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getAggregateEnrichmentView()
 * @model extendedMetaData="name='AggregateEnrichmentView' kind='elementOnly'"
 * @generated
 */
public interface AggregateEnrichmentView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Functions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Aggregated ColumnFunctions
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functions</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getAggregateEnrichmentView_Functions()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='functions'"
	 * @generated
	 */
	EList<AggregateColumnFunction> getFunctions();

	/**
	 * Returns the value of the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The GroupBy clause which includes the GroupBy columns
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Group By Clause</em>' containment reference.
	 * @see #setGroupByClause(GroupByClause)
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getAggregateEnrichmentView_GroupByClause()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='groupByClause'"
	 * @generated
	 */
	GroupByClause getGroupByClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getGroupByClause <em>Group By Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Group By Clause</em>' containment reference.
	 * @see #getGroupByClause()
	 * @generated
	 */
	void setGroupByClause(GroupByClause value);

} // AggregateEnrichmentView
