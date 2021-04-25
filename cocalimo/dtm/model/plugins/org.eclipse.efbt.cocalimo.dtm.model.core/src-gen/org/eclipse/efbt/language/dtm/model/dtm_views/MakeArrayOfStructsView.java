/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Make Array Of Structs View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getGroupingVariable <em>Grouping Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getSourceStructVariable <em>Source Struct Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getTargetArrayVariable <em>Target Array Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeArrayOfStructsView()
 * @model
 * @generated
 */
public interface MakeArrayOfStructsView extends DTMView {
	/**
	 * Returns the value of the '<em><b>Grouping Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Grouping Variable</em>' reference.
	 * @see #setGroupingVariable(VARIABLE)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeArrayOfStructsView_GroupingVariable()
	 * @model
	 * @generated
	 */
	VARIABLE getGroupingVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getGroupingVariable <em>Grouping Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Grouping Variable</em>' reference.
	 * @see #getGroupingVariable()
	 * @generated
	 */
	void setGroupingVariable(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Source Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Struct Variable</em>' reference.
	 * @see #setSourceStructVariable(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeArrayOfStructsView_SourceStructVariable()
	 * @model
	 * @generated
	 */
	StructTypedVariable getSourceStructVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getSourceStructVariable <em>Source Struct Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source Struct Variable</em>' reference.
	 * @see #getSourceStructVariable()
	 * @generated
	 */
	void setSourceStructVariable(StructTypedVariable value);

	/**
	 * Returns the value of the '<em><b>Target Array Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Array Variable</em>' reference.
	 * @see #setTargetArrayVariable(ArrayTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeArrayOfStructsView_TargetArrayVariable()
	 * @model
	 * @generated
	 */
	ArrayTypedVariable getTargetArrayVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getTargetArrayVariable <em>Target Array Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Array Variable</em>' reference.
	 * @see #getTargetArrayVariable()
	 * @generated
	 */
	void setTargetArrayVariable(ArrayTypedVariable value);

} // MakeArrayOfStructsView
