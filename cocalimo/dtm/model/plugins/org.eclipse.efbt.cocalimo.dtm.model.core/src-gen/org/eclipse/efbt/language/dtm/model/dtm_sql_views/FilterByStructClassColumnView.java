/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Filter By Struct Class Column View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getRenameAsStructColumn <em>Rename As Struct Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getStructClass <em>Struct Class</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getStructColumn <em>Struct Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByStructClassColumnView()
 * @model
 * @generated
 */
public interface FilterByStructClassColumnView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Rename As Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rename As Struct Column</em>' reference.
	 * @see #setRenameAsStructColumn(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByStructClassColumnView_RenameAsStructColumn()
	 * @model
	 * @generated
	 */
	StructTypedVariable getRenameAsStructColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getRenameAsStructColumn <em>Rename As Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Rename As Struct Column</em>' reference.
	 * @see #getRenameAsStructColumn()
	 * @generated
	 */
	void setRenameAsStructColumn(StructTypedVariable value);

	/**
	 * Returns the value of the '<em><b>Struct Class</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Struct Class</em>' reference.
	 * @see #setStructClass(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByStructClassColumnView_StructClass()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getStructClass();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getStructClass <em>Struct Class</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Struct Class</em>' reference.
	 * @see #getStructClass()
	 * @generated
	 */
	void setStructClass(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Struct Column</em>' reference.
	 * @see #setStructColumn(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getFilterByStructClassColumnView_StructColumn()
	 * @model
	 * @generated
	 */
	StructTypedVariable getStructColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView#getStructColumn <em>Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Struct Column</em>' reference.
	 * @see #getStructColumn()
	 * @generated
	 */
	void setStructColumn(StructTypedVariable value);

} // FilterByStructClassColumnView
