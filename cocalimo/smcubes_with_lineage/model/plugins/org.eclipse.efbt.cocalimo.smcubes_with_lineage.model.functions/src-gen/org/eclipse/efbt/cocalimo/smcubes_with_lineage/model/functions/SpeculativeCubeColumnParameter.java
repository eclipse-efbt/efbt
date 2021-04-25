/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Speculative Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter where we are taking a CubeColumn from a cube.
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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getSpeculativeCubeColumnParameter()
 * @model extendedMetaData="name='SpeculativeCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface SpeculativeCubeColumnParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Column of the desired cube Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getSpeculativeCubeColumnParameter_Column()
	 * @model extendedMetaData="kind='attribute' name='column'"
	 * @generated
	 */
	VARIABLE getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}' reference.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getSpeculativeCubeColumnParameter_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

} // SpeculativeCubeColumnParameter
