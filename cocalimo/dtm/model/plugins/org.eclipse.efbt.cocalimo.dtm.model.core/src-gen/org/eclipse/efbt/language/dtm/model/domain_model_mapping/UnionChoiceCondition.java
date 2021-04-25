/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Union Choice Condition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition#getChosenCubes <em>Chosen Cubes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getUnionChoiceCondition()
 * @model
 * @generated
 */
public interface UnionChoiceCondition extends CodeCondition {
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getUnionChoiceCondition_Cube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Chosen Cubes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Chosen Cubes</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getUnionChoiceCondition_ChosenCubes()
	 * @model
	 * @generated
	 */
	EList<FreeBirdToolsCube> getChosenCubes();

} // UnionChoiceCondition
