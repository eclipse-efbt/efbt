/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Get Struct Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a ColumnFunction which represents the retrieval of a the value of a single Variable that exists inside  a CubeColumn that contains a StructTypeVariable.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getGetStructColumnFunction()
 * @model
 * @generated
 */
public interface GetStructColumnFunction extends ColumnFunction {
	/**
	 * Returns the value of the '<em><b>Column In Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Variable that exists inside  a CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column In Struct</em>' reference.
	 * @see #setColumnInStruct(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getGetStructColumnFunction_ColumnInStruct()
	 * @model
	 * @generated
	 */
	VARIABLE getColumnInStruct();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column In Struct</em>' reference.
	 * @see #getColumnInStruct()
	 * @generated
	 */
	void setColumnInStruct(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(CubeColumn)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getGetStructColumnFunction_CubeColumn()
	 * @model
	 * @generated
	 */
	CubeColumn getCubeColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(CubeColumn value);

} // GetStructColumnFunction
