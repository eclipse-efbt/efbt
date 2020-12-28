/**
 */
package org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions;

import org.eclipse.efbt.cocamo.functions.model.functions.Parameter;

import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.lineage.common.model.lineagecubes.efbt_advanced_data_definition.StructTypedVariable;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Speculative Struct Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Speculative parameter, which relates to a variable within a CubeColumn, where we expect that the CubeColumn holds a StructTypedVariable 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn <em>Struct Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getSpeculativeStructColumnParameter()
 * @model
 * @generated
 */
public interface SpeculativeStructColumnParameter extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The variable that we are interested in from  inside the StructTypedVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column Inside Struct</em>' reference.
	 * @see #setColumnInsideStruct(VARIABLE)
	 * @see org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getSpeculativeStructColumnParameter_ColumnInsideStruct()
	 * @model
	 * @generated
	 */
	VARIABLE getColumnInsideStruct();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column Inside Struct</em>' reference.
	 * @see #getColumnInsideStruct()
	 * @generated
	 */
	void setColumnInsideStruct(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cube that we expect to hold the StructTypedVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getSpeculativeStructColumnParameter_Cube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The StructTypedVariable that we speculate exists in the cube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Struct Column</em>' reference.
	 * @see #setStructColumn(StructTypedVariable)
	 * @see org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getSpeculativeStructColumnParameter_StructColumn()
	 * @model
	 * @generated
	 */
	StructTypedVariable getStructColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn <em>Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Struct Column</em>' reference.
	 * @see #getStructColumn()
	 * @generated
	 */
	void setStructColumn(StructTypedVariable value);

} // SpeculativeStructColumnParameter
