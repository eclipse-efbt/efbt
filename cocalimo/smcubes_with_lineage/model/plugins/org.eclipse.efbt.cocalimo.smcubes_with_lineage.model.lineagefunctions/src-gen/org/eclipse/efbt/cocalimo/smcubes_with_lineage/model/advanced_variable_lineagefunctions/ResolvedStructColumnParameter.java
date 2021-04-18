/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Parameter;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Resolved Struct Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Resolved parameter, which relates to a variable within a resolved CubeColumn, where the CubeColumn holds a StructTypeVaraible 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter()
 * @model
 * @generated
 */
public interface ResolvedStructColumnParameter extends Parameter
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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter_ColumnInsideStruct()
	 * @model
	 * @generated
	 */
	VARIABLE getColumnInsideStruct();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column Inside Struct</em>' reference.
	 * @see #getColumnInsideStruct()
	 * @generated
	 */
	void setColumnInsideStruct(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn that contains the StructTypedVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(CubeColumn)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter_CubeColumn()
	 * @model
	 * @generated
	 */
	CubeColumn getCubeColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(CubeColumn value);

} // ResolvedStructColumnParameter
