/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;

import org.eclipse.emf.ecore.EEnumLiteral;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Heirarchical Enum Literal</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.HeirarchicalEnumLiteral#getParentLiteral <em>Parent Literal</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getHeirarchicalEnumLiteral()
 * @model
 * @generated
 */
public interface HeirarchicalEnumLiteral extends EEnumLiteral {
	/**
	 * Returns the value of the '<em><b>Parent Literal</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent Literal</em>' reference.
	 * @see #setParentLiteral(EEnumLiteral)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getHeirarchicalEnumLiteral_ParentLiteral()
	 * @model
	 * @generated
	 */
	EEnumLiteral getParentLiteral();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.HeirarchicalEnumLiteral#getParentLiteral <em>Parent Literal</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent Literal</em>' reference.
	 * @see #getParentLiteral()
	 * @generated
	 */
	void setParentLiteral(EEnumLiteral value);

} // HeirarchicalEnumLiteral
