/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Feature Group</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A group of ColumnFunctions for a Cube.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup#getDerivedFeatures <em>Derived Features</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedFeatureGroup()
 * @model
 * @generated
 */
public interface DerivedFeatureGroup extends EObject {
	/**
	 * Returns the value of the '<em><b>Derived Features</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The group of ColumnFunctions for a Cube.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Derived Features</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getDerivedFeatureGroup_DerivedFeatures()
	 * @model containment="true"
	 * @generated
	 */
	EList<DerivedAttribute> getDerivedFeatures();

} // DerivedFeatureGroup
