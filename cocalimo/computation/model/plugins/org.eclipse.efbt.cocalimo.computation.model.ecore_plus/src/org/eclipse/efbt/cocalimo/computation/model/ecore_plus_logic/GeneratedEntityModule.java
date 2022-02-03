/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generated Entity Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule#getGeneratedEntities <em>Generated Entities</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGeneratedEntityModule()
 * @model
 * @generated
 */
public interface GeneratedEntityModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Generated Entities</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Generated Entities</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGeneratedEntityModule_GeneratedEntities()
	 * @model containment="true"
	 * @generated
	 */
	EList<GeneratedEntity> getGeneratedEntities();

} // GeneratedEntityModule
