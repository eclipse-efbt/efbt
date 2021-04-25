/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Explode Array Of Structs View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getArraySourceColumn <em>Array Source Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getIndexColumn <em>Index Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getTargetStructColumn <em>Target Struct Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getExplodeArrayOfStructsView()
 * @model
 * @generated
 */
public interface ExplodeArrayOfStructsView extends DTMView {
	/**
	 * Returns the value of the '<em><b>Array Source Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Array Source Column</em>' reference.
	 * @see #setArraySourceColumn(ArrayTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getExplodeArrayOfStructsView_ArraySourceColumn()
	 * @model
	 * @generated
	 */
	ArrayTypedVariable getArraySourceColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getArraySourceColumn <em>Array Source Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Array Source Column</em>' reference.
	 * @see #getArraySourceColumn()
	 * @generated
	 */
	void setArraySourceColumn(ArrayTypedVariable value);

	/**
	 * Returns the value of the '<em><b>Index Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Index Column</em>' reference.
	 * @see #setIndexColumn(VARIABLE)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getExplodeArrayOfStructsView_IndexColumn()
	 * @model
	 * @generated
	 */
	VARIABLE getIndexColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getIndexColumn <em>Index Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Index Column</em>' reference.
	 * @see #getIndexColumn()
	 * @generated
	 */
	void setIndexColumn(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Target Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Struct Column</em>' reference.
	 * @see #setTargetStructColumn(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getExplodeArrayOfStructsView_TargetStructColumn()
	 * @model
	 * @generated
	 */
	StructTypedVariable getTargetStructColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getTargetStructColumn <em>Target Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Struct Column</em>' reference.
	 * @see #getTargetStructColumn()
	 * @generated
	 */
	void setTargetStructColumn(StructTypedVariable value);

} // ExplodeArrayOfStructsView
