/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Versioned SQL Views Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module of SQLViews with a version number
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule#getSqlViews <em>Sql Views</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getVersionedSQLViewsModule()
 * @model extendedMetaData="name='VersionedSQLViewsModule' kind='elementOnly'"
 * @generated
 */
public interface VersionedSQLViewsModule extends VersionedFunctionalModuleLogic
{
	/**
	 * Returns the value of the '<em><b>Sql Views</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The SQLViews in the module.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sql Views</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getVersionedSQLViewsModule_SqlViews()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='sqlViews'"
	 * @generated
	 */
	EList<SQLView> getSqlViews();

} // VersionedSQLViewsModule
