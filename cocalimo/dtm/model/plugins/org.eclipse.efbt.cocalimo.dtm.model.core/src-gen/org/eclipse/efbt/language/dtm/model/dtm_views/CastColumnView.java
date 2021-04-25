/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cast Column View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getSourceStructVariable <em>Source Struct Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getTargetStructVariable <em>Target Struct Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getCastColumnView()
 * @model
 * @generated
 */
public interface CastColumnView extends DTMView {
	/**
	 * Returns the value of the '<em><b>Source Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Struct Variable</em>' reference.
	 * @see #setSourceStructVariable(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getCastColumnView_SourceStructVariable()
	 * @model
	 * @generated
	 */
	StructTypedVariable getSourceStructVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getSourceStructVariable <em>Source Struct Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source Struct Variable</em>' reference.
	 * @see #getSourceStructVariable()
	 * @generated
	 */
	void setSourceStructVariable(StructTypedVariable value);

	/**
	 * Returns the value of the '<em><b>Target Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Struct Variable</em>' reference.
	 * @see #setTargetStructVariable(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getCastColumnView_TargetStructVariable()
	 * @model
	 * @generated
	 */
	StructTypedVariable getTargetStructVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getTargetStructVariable <em>Target Struct Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Struct Variable</em>' reference.
	 * @see #getTargetStructVariable()
	 * @generated
	 */
	void setTargetStructVariable(StructTypedVariable value);

} // CastColumnView
