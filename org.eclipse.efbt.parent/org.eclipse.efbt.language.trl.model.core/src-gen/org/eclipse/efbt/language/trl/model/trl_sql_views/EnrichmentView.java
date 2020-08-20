/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Enrichment View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView which represents a Copy of all rows of the single source cube of the associated DerivedCube,
 *       and adds one or more calculated columns. 
 *       The resulting cube structure will be the same as that source cube, with one column added per calculated column,
 *       although we can choose to Rename some of the columns from the source cube if we wish.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getRenames <em>Renames</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getEnrichmentView()
 * @model extendedMetaData="name='EnrichmentView' kind='elementOnly'"
 * @generated
 */
public interface EnrichmentView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Functions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The ColumnFunctions describing how to derive the new columns. Note that their will be a  one to 
	 *               one relationship between the rows in the resulting view and the rows in the source row. So the column function could  
	 *               represent the  addition of 2 columns in the source row for example.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functions</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getEnrichmentView_Functions()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='functions'"
	 * @generated
	 */
	EList<BasicColumnFunction> getFunctions();

	/**
	 * Returns the value of the '<em><b>Renames</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A set of Renames, these are similar is concept to the AS command in SQL,
	 *                e.g. in SQL select CCY as Currency from cube1, will mean that the cube structure of the resulting view   has a column called Currency  not CCY, and that cube1 naturally remains with having a column called CCY.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Renames</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getEnrichmentView_Renames()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='renames'"
	 * @generated
	 */
	EList<RenameAs> getRenames();

} // EnrichmentView
