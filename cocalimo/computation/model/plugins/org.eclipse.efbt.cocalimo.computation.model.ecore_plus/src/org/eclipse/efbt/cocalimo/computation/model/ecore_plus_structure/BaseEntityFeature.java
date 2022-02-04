/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;

import org.eclipse.emf.ecore.EAttribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Entity Feature</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature#isIsFK <em>Is FK</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature#getFkEntity <em>Fk Entity</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntityFeature()
 * @model
 * @generated
 */
public interface BaseEntityFeature extends EAttribute {
	/**
	 * Returns the value of the '<em><b>Is FK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is FK</em>' attribute.
	 * @see #setIsFK(boolean)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntityFeature_IsFK()
	 * @model
	 * @generated
	 */
	boolean isIsFK();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature#isIsFK <em>Is FK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is FK</em>' attribute.
	 * @see #isIsFK()
	 * @generated
	 */
	void setIsFK(boolean value);

	/**
	 * Returns the value of the '<em><b>Fk Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Fk Entity</em>' reference.
	 * @see #setFkEntity(BaseEntity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntityFeature_FkEntity()
	 * @model
	 * @generated
	 */
	BaseEntity getFkEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature#getFkEntity <em>Fk Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Fk Entity</em>' reference.
	 * @see #getFkEntity()
	 * @generated
	 */
	void setFkEntity(BaseEntity value);

} // BaseEntityFeature
