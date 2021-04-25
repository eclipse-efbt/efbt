/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Make Struct View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView#getTargetVariable <em>Target Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeStructView()
 * @model
 * @generated
 */
public interface MakeStructView extends DTMView {
	/**
	 * Returns the value of the '<em><b>Target Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Variable</em>' reference.
	 * @see #setTargetVariable(StructTypedVariable)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getMakeStructView_TargetVariable()
	 * @model
	 * @generated
	 */
	StructTypedVariable getTargetVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView#getTargetVariable <em>Target Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Variable</em>' reference.
	 * @see #getTargetVariable()
	 * @generated
	 */
	void setTargetVariable(StructTypedVariable value);

} // MakeStructView
