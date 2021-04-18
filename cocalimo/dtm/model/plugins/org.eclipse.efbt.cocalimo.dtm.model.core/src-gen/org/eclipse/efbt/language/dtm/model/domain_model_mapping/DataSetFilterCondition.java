/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BooleanFunction;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Data Set Filter Condition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition#getFilterConditions <em>Filter Conditions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDataSetFilterCondition()
 * @model
 * @generated
 */
public interface DataSetFilterCondition extends CodeCondition
{
	/**
	 * Returns the value of the '<em><b>Filter Conditions</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Filter Conditions</em>' containment reference.
	 * @see #setFilterConditions(BooleanFunction)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDataSetFilterCondition_FilterConditions()
	 * @model containment="true"
	 * @generated
	 */
	BooleanFunction getFilterConditions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition#getFilterConditions <em>Filter Conditions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Filter Conditions</em>' containment reference.
	 * @see #getFilterConditions()
	 * @generated
	 */
	void setFilterConditions(BooleanFunction value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getDataSetFilterCondition_Cube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

} // DataSetFilterCondition
