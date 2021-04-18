/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Explode Struct View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeStructView#getColumnToUnpack <em>Column To Unpack</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getExplodeStructView()
 * @model
 * @generated
 */
public interface ExplodeStructView extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Column To Unpack</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column To Unpack</em>' reference.
	 * @see #setColumnToUnpack(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getExplodeStructView_ColumnToUnpack()
	 * @model
	 * @generated
	 */
	StructTypedVariable getColumnToUnpack();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeStructView#getColumnToUnpack <em>Column To Unpack</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column To Unpack</em>' reference.
	 * @see #getColumnToUnpack()
	 * @generated
	 */
	void setColumnToUnpack(StructTypedVariable value);

} // ExplodeStructView
