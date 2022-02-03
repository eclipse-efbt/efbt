/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getSourceEntities <em>Source Entities</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedAttributes <em>Derived Attributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getRowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedStructs <em>Derived Structs</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity()
 * @model
 * @generated
 */
public interface DerivedEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>Source Entities</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Entities</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity_SourceEntities()
	 * @model
	 * @generated
	 */
	EList<Entity> getSourceEntities();

	/**
	 * Returns the value of the '<em><b>Derived Attributes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Derived Attributes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity_DerivedAttributes()
	 * @model containment="true"
	 * @generated
	 */
	EList<DerivedAttribute> getDerivedAttributes();

	/**
	 * Returns the value of the '<em><b>Row Creation Approach For Entity</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Row Creation Approach For Entity</em>' containment reference.
	 * @see #setRowCreationApproachForEntity(RowCreationApproachForEntity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity_RowCreationApproachForEntity()
	 * @model containment="true"
	 * @generated
	 */
	RowCreationApproachForEntity getRowCreationApproachForEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getRowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row Creation Approach For Entity</em>' containment reference.
	 * @see #getRowCreationApproachForEntity()
	 * @generated
	 */
	void setRowCreationApproachForEntity(RowCreationApproachForEntity value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity_Cube()
	 * @model
	 * @generated
	 */
	CUBE getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(CUBE value);

	/**
	 * Returns the value of the '<em><b>Derived Structs</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Derived Structs</em>' containment reference.
	 * @see #setDerivedStructs(DerivedStruct)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntity_DerivedStructs()
	 * @model containment="true"
	 * @generated
	 */
	DerivedStruct getDerivedStructs();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedStructs <em>Derived Structs</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Derived Structs</em>' containment reference.
	 * @see #getDerivedStructs()
	 * @generated
	 */
	void setDerivedStructs(DerivedStruct value);

} // DerivedEntity
