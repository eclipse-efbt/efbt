/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Entity Table</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getSourceTables <em>Source Tables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getContainedEntityType <em>Contained Entity Type</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntityTable()
 * @model
 * @generated
 */
public interface DerivedEntityTable extends EntityTable {
	/**
	 * Returns the value of the '<em><b>Source Tables</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Tables</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntityTable_SourceTables()
	 * @model
	 * @generated
	 */
	EList<EntityTable> getSourceTables();

	/**
	 * Returns the value of the '<em><b>Contained Entity Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Contained Entity Type</em>' reference.
	 * @see #setContainedEntityType(DerivedEntity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedEntityTable_ContainedEntityType()
	 * @model
	 * @generated
	 */
	DerivedEntity getContainedEntityType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getContainedEntityType <em>Contained Entity Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Contained Entity Type</em>' reference.
	 * @see #getContainedEntityType()
	 * @generated
	 */
	void setContainedEntityType(DerivedEntity value);

} // DerivedEntityTable
