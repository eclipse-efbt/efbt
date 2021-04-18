/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Combination Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module storing a list of Combinations, and a list of related Combination Items, and a list of CUBE_TO_COMBINATIONs
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombinations <em>Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombination_items <em>Combination items</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCubeToCombinationLinks <em>Cube To Combination Links</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCombinationModule()
 * @model extendedMetaData="name='CombinationModule' kind='elementOnly'"
 * @generated
 */
public interface CombinationModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Combinations</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of Combinations
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Combinations</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCombinationModule_Combinations()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='combinations'"
	 * @generated
	 */
	EList<COMBINATION> getCombinations();

	/**
	 * Returns the value of the '<em><b>Combination items</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of Combination items
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Combination items</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCombinationModule_Combination_items()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='combination_items'"
	 * @generated
	 */
	EList<COMBINATION_ITEM> getCombination_items();

	/**
	 * Returns the value of the '<em><b>Cube To Combination Links</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of CUBE_TO_COMBINATIONs providing links between the cubes and combinations
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube To Combination Links</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCombinationModule_CubeToCombinationLinks()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubeToCombinationLinks'"
	 * @generated
	 */
	EList<CUBE_TO_COMBINATION> getCubeToCombinationLinks();

} // CombinationModule
