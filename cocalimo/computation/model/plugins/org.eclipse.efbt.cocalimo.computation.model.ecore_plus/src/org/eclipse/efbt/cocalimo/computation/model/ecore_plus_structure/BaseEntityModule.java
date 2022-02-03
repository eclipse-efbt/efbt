/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Entity Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityModule#getBaseEntities <em>Base Entities</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntityModule()
 * @model
 * @generated
 */
public interface BaseEntityModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Base Entities</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Base Entities</em>' containment reference.
	 * @see #setBaseEntities(BaseEntity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#getBaseEntityModule_BaseEntities()
	 * @model containment="true"
	 * @generated
	 */
	BaseEntity getBaseEntities();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityModule#getBaseEntities <em>Base Entities</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Base Entities</em>' containment reference.
	 * @see #getBaseEntities()
	 * @generated
	 */
	void setBaseEntities(BaseEntity value);

} // BaseEntityModule
