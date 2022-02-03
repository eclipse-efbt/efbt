/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity#getBaseFeatures <em>Base Features</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntity()
 * @model
 * @generated
 */
public interface BaseEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>Base Features</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Base Features</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntity_BaseFeatures()
	 * @model containment="true"
	 * @generated
	 */
	EList<BaseEntityFeature> getBaseFeatures();

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntity_Cube()
	 * @model
	 * @generated
	 */
	CUBE getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(CUBE value);

} // BaseEntity
