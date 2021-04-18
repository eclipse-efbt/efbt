/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Struct Typed Variable</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An Variable which is efined as group of variables. We dpo not specify the group of variables, but rather we sate that they should be the same as the variables associated with a BaseCube.  Not that it is CubeSchemas which allow us to determine which Varaibles are linked to a BaseCube . 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable#getCubeType <em>Cube Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getStructTypedVariable()
 * @model
 * @generated
 */
public interface StructTypedVariable extends AdvancedTypedVariable
{
	/**
	 * Returns the value of the '<em><b>Cube Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The BaseCube associated with the StructVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Type</em>' reference.
	 * @see #setCubeType(BaseCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getStructTypedVariable_CubeType()
	 * @model
	 * @generated
	 */
	BaseCube getCubeType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable#getCubeType <em>Cube Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Type</em>' reference.
	 * @see #getCubeType()
	 * @generated
	 */
	void setCubeType(BaseCube value);

} // StructTypedVariable
