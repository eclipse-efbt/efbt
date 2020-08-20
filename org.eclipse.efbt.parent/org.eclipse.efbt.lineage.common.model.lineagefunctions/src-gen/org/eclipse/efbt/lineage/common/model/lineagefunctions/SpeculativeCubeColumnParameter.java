/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.efbt.common.model.functions.Parameter;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Speculative Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter where we are taking a CubeColumn from a cube.
 * Note that functions are always part of RowCreationApproachs or cubeColumnFunctions, both of these are associated with derived cubes,  and  we only allow to take  CubeColumns for source cubes of the derived cube.
 *  Speculative here relates to the fact that this cubeColumn may or may not actually exist. 
 *  Note that during development of a program that set of actually existing CubeColumns changes.
 *  Allowing a specultiveCubeColumnParameter    means that we can develop more quickly, and run checks occasionally to check that all SpeculativeCubeColumnParameters can be resolved  to real CubeColumns.
 *                
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getSpeculativeCubeColumnParameter()
 * @model extendedMetaData="name='SpeculativeCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface SpeculativeCubeColumnParameter extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Column of the desired cube Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(VARIABLE)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getSpeculativeCubeColumnParameter_Column()
	 * @model extendedMetaData="kind='attribute' name='column'"
	 * @generated
	 */
	VARIABLE getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cube of the desired cube Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getSpeculativeCubeColumnParameter_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

} // SpeculativeCubeColumnParameter
