/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Struct</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedStruct()
 * @model
 * @generated
 */
public interface DerivedStruct extends EReference {
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedStruct_Cube()
	 * @model
	 * @generated
	 */
	CUBE getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(CUBE value);

} // DerivedStruct
