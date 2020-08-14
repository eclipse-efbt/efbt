/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage
 * @generated
 */
public interface Trl_sql_viewsFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Trl_sql_viewsFactory eINSTANCE = org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Aggregate Enrichment View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Aggregate Enrichment View</em>'.
	 * @generated
	 */
	AggregateEnrichmentView createAggregateEnrichmentView();

	/**
	 * Returns a new object of class '<em>Copy View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Copy View</em>'.
	 * @generated
	 */
	CopyView createCopyView();

	/**
	 * Returns a new object of class '<em>Enrichment View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Enrichment View</em>'.
	 * @generated
	 */
	EnrichmentView createEnrichmentView();

	/**
	 * Returns a new object of class '<em>Filter By Condition View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Filter By Condition View</em>'.
	 * @generated
	 */
	FilterByConditionView createFilterByConditionView();

	/**
	 * Returns a new object of class '<em>Generic View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generic View</em>'.
	 * @generated
	 */
	GenericView createGenericView();

	/**
	 * Returns a new object of class '<em>Group By Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Group By Clause</em>'.
	 * @generated
	 */
	GroupByClause createGroupByClause();

	/**
	 * Returns a new object of class '<em>Ignore Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Ignore Column</em>'.
	 * @generated
	 */
	IgnoreColumn createIgnoreColumn();

	/**
	 * Returns a new object of class '<em>Join View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Join View</em>'.
	 * @generated
	 */
	JoinView createJoinView();

	/**
	 * Returns a new object of class '<em>Rename As</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Rename As</em>'.
	 * @generated
	 */
	RenameAs createRenameAs();

	/**
	 * Returns a new object of class '<em>Select Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Clause</em>'.
	 * @generated
	 */
	SelectClause createSelectClause();

	/**
	 * Returns a new object of class '<em>SQL View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SQL View</em>'.
	 * @generated
	 */
	SQLView createSQLView();

	/**
	 * Returns a new object of class '<em>Union View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Union View</em>'.
	 * @generated
	 */
	UnionView createUnionView();

	/**
	 * Returns a new object of class '<em>Versioned SQL Views Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Versioned SQL Views Module</em>'.
	 * @generated
	 */
	VersionedSQLViewsModule createVersionedSQLViewsModule();

	/**
	 * Returns a new object of class '<em>Where Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Where Clause</em>'.
	 * @generated
	 */
	WhereClause createWhereClause();

	/**
	 * Returns a new object of class '<em>Base View Incorporating Deltas</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base View Incorporating Deltas</em>'.
	 * @generated
	 */
	BaseViewIncorporatingDeltas createBaseViewIncorporatingDeltas();

	/**
	 * Returns a new object of class '<em>Delta Accumulation</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Delta Accumulation</em>'.
	 * @generated
	 */
	DeltaAccumulation createDeltaAccumulation();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Trl_sql_viewsPackage getTrl_sql_viewsPackage();

} //Trl_sql_viewsFactory
