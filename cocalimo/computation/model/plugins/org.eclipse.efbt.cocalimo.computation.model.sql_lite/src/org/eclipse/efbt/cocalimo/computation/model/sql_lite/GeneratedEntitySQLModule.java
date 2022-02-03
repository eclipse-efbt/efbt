/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generated Entity SQL Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.GeneratedEntitySQLModule#getGeneratedEntitySQLs <em>Generated Entity SQ Ls</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getGeneratedEntitySQLModule()
 * @model
 * @generated
 */
public interface GeneratedEntitySQLModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Generated Entity SQ Ls</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.GeneratedEntitySQL}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Generated Entity SQ Ls</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getGeneratedEntitySQLModule_GeneratedEntitySQLs()
	 * @model containment="true"
	 * @generated
	 */
	EList<GeneratedEntitySQL> getGeneratedEntitySQLs();

} // GeneratedEntitySQLModule
