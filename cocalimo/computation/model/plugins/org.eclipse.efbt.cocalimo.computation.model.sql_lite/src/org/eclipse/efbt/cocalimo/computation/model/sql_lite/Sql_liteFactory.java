/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage
 * @generated
 */
public interface Sql_liteFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Sql_liteFactory eINSTANCE = org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.Sql_liteFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>View</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>View</em>'.
	 * @generated
	 */
	View createView();

	/**
	 * Returns a new object of class '<em>Select Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Clause</em>'.
	 * @generated
	 */
	SelectClause createSelectClause();

	/**
	 * Returns a new object of class '<em>Column As Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column As Function</em>'.
	 * @generated
	 */
	ColumnAsFunction createColumnAsFunction();

	/**
	 * Returns a new object of class '<em>Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column</em>'.
	 * @generated
	 */
	Column createColumn();

	/**
	 * Returns a new object of class '<em>Where Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Where Clause</em>'.
	 * @generated
	 */
	WhereClause createWhereClause();

	/**
	 * Returns a new object of class '<em>Table</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Table</em>'.
	 * @generated
	 */
	Table createTable();

	/**
	 * Returns a new object of class '<em>SQL Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SQL Entity</em>'.
	 * @generated
	 */
	SQLEntity createSQLEntity();

	/**
	 * Returns a new object of class '<em>From Clause</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>From Clause</em>'.
	 * @generated
	 */
	FromClause createFromClause();

	/**
	 * Returns a new object of class '<em>SQL Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SQL Entity Module</em>'.
	 * @generated
	 */
	SQLEntityModule createSQLEntityModule();

	/**
	 * Returns a new object of class '<em>Generated Entity SQL</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generated Entity SQL</em>'.
	 * @generated
	 */
	GeneratedEntitySQL createGeneratedEntitySQL();

	/**
	 * Returns a new object of class '<em>Generated Entity SQL Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generated Entity SQL Module</em>'.
	 * @generated
	 */
	GeneratedEntitySQLModule createGeneratedEntitySQLModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Sql_litePackage getSql_litePackage();

} //Sql_liteFactory
