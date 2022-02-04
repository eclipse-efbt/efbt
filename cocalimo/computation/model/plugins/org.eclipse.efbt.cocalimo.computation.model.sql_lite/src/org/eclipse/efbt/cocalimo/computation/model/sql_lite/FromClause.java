/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>From Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.FromClause#getSqlEntities <em>Sql Entities</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getFromClause()
 * @model
 * @generated
 */
public interface FromClause extends EObject {
	/**
	 * Returns the value of the '<em><b>Sql Entities</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sql Entities</em>' reference.
	 * @see #setSqlEntities(SQLEntity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getFromClause_SqlEntities()
	 * @model
	 * @generated
	 */
	SQLEntity getSqlEntities();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.FromClause#getSqlEntities <em>Sql Entities</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sql Entities</em>' reference.
	 * @see #getSqlEntities()
	 * @generated
	 */
	void setSqlEntities(SQLEntity value);

} // FromClause
