/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Explode Array Of Structs Row Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getExplodeArrayOfStructsRowFunction()
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
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getExplodeArrayOfStructsRowFunction_ArraySourceVariable()
	 * @model
	 * @generated
	 */
	EStructuralFeature getArraySourceVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Array Source Variable</em>' reference.
	 * @see #getArraySourceVariable()
	 * @generated
	 */
	void setArraySourceVariable(EStructuralFeature value);

} // ExplodeArrayOfStructsRowFunction
