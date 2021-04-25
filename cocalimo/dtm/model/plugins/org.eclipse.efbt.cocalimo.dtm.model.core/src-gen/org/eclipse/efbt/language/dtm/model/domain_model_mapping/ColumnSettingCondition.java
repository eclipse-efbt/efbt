/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Column Setting Condition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getVariable <em>Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getFilterConditions <em>Filter Conditions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getColumnSettingCondition()
 * @model
 * @generated
 */
public interface ColumnSettingCondition extends CodeCondition {
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getColumnSettingCondition_Cube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VARIABLE)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getColumnSettingCondition_Variable()
	 * @model
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Filter Conditions</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Filter Conditions</em>' containment reference.
	 * @see #setFilterConditions(BasicFunction)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getColumnSettingCondition_FilterConditions()
	 * @model containment="true"
	 * @generated
	 */
	BasicFunction getFilterConditions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition#getFilterConditions <em>Filter Conditions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Filter Conditions</em>' containment reference.
	 * @see #getFilterConditions()
	 * @generated
	 */
	void setFilterConditions(BasicFunction value);

} // ColumnSettingCondition
