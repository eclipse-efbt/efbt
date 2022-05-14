/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SQL Entity Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule#getSqlEntities <em>Sql Entities</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage#getSQLEntityModule()
 * @model
 * @generated
 */
public interface SQLEntityModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Sql Entities</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sql Entities</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage#getSQLEntityModule_SqlEntities()
	 * @model containment="true"
	 * @generated
	 */
	EList<SQLEntity> getSqlEntities();

} // SQLEntityModule
