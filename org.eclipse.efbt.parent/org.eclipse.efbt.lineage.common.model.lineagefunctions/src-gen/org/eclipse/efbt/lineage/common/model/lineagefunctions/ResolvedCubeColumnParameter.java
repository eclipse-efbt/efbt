/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.efbt.common.model.functions.Parameter;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Resolved Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Cube Column parameter, this is a reference to a real existing CubeColumn.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getResolvedCubeColumnParameter()
 * @model extendedMetaData="name='ResolvedCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface ResolvedCubeColumnParameter extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * TheCubeColumn this is a reference to a real existing cubeColumn.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(CubeColumn)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getResolvedCubeColumnParameter_CubeColumn()
	 * @model extendedMetaData="kind='attribute' name='cubeColumn'"
	 * @generated
	 */
	CubeColumn getCubeColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(CubeColumn value);

} // ResolvedCubeColumnParameter
