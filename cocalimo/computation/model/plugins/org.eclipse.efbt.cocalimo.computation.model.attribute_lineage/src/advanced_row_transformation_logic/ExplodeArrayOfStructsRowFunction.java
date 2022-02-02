/**
 */
package advanced_row_transformation_logic;

import org.eclipse.emf.ecore.EStructuralFeature;

import row_transformation_logic.RowCreationApproach;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Explode Array Of Structs Row Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}</li>
 * </ul>
 *
 * @see advanced_row_transformation_logic.Advanced_row_transformation_logicPackage#getExplodeArrayOfStructsRowFunction()
 * @model
 * @generated
 */
public interface ExplodeArrayOfStructsRowFunction extends RowCreationApproach {
	/**
	 * Returns the value of the '<em><b>Array Source Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Array Source Variable</em>' reference.
	 * @see #setArraySourceVariable(EStructuralFeature)
	 * @see advanced_row_transformation_logic.Advanced_row_transformation_logicPackage#getExplodeArrayOfStructsRowFunction_ArraySourceVariable()
	 * @model
	 * @generated
	 */
	EStructuralFeature getArraySourceVariable();

	/**
	 * Sets the value of the '{@link advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Array Source Variable</em>' reference.
	 * @see #getArraySourceVariable()
	 * @generated
	 */
	void setArraySourceVariable(EStructuralFeature value);

} // ExplodeArrayOfStructsRowFunction
