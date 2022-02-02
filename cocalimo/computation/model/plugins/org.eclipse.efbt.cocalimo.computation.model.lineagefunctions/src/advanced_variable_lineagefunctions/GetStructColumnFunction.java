/**
 */
package advanced_variable_lineagefunctions;

import column_transformation_logic.ColumnFunction;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

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
 *   <li>{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}</li>
 *   <li>{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getGetStructColumnFunction()
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
	 * @see #setColumnInStruct(EAttribute)
	 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getGetStructColumnFunction_ColumnInStruct()
	 * @model
	 * @generated
	 */
	EAttribute getColumnInStruct();

	/**
	 * Sets the value of the '{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column In Struct</em>' reference.
	 * @see #getColumnInStruct()
	 * @generated
	 */
	void setColumnInStruct(EAttribute value);

	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(EStructuralFeature)
	 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getGetStructColumnFunction_CubeColumn()
	 * @model
	 * @generated
	 */
	EStructuralFeature getCubeColumn();

	/**
	 * Sets the value of the '{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(EStructuralFeature value);

} // GetStructColumnFunction
