/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Array Typed Variable</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Variable which is an Array of Structs, where the structs have type defined by the cubeType.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable#getCubeType <em>Cube Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getArrayTypedVariable()
 * @model
 * @generated
 */
public interface ArrayTypedVariable extends AdvancedTypedVariable
{
	/**
	 * Returns the value of the '<em><b>Cube Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A cube which describes the type of structs held in the array
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Type</em>' reference.
	 * @see #setCubeType(BaseCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getArrayTypedVariable_CubeType()
	 * @model
	 * @generated
	 */
	BaseCube getCubeType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable#getCubeType <em>Cube Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Type</em>' reference.
	 * @see #getCubeType()
	 * @generated
	 */
	void setCubeType(BaseCube value);

} // ArrayTypedVariable
