/**
 */
package advanced_variable_lineagefunctions;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EStructuralFeature;

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
 *   <li>{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter()
 * @model
 * @generated
 */
public interface ResolvedStructColumnParameter extends EParameter {
	/**
	 * Returns the value of the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The variable that we are interested in from  inside the StructTypedVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column Inside Struct</em>' reference.
	 * @see #setColumnInsideStruct(EAttribute)
	 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter_ColumnInsideStruct()
	 * @model
	 * @generated
	 */
	EAttribute getColumnInsideStruct();

	/**
	 * Sets the value of the '{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column Inside Struct</em>' reference.
	 * @see #getColumnInsideStruct()
	 * @generated
	 */
	void setColumnInsideStruct(EAttribute value);

	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn that contains the StructTypedVariable
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(EStructuralFeature)
	 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage#getResolvedStructColumnParameter_CubeColumn()
	 * @model
	 * @generated
	 */
	EStructuralFeature getCubeColumn();

	/**
	 * Sets the value of the '{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(EStructuralFeature value);

} // ResolvedStructColumnParameter
