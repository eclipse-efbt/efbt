/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cube Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module storing a list of CUBEs and a list of CUBE_STRUCTURES and a list of CUBE_STRUCTURE_ITEMS
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubes <em>Cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructures <em>Cube Structures</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructureItems <em>Cube Structure Items</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCubeModule()
 * @model extendedMetaData="name='CubeModule' kind='elementOnly'"
 * @generated
 */
public interface CubeModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Cubes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of CUBEs
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cubes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCubeModule_Cubes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubes'"
	 * @generated
	 */
	EList<CUBE> getCubes();

	/**
	 * Returns the value of the '<em><b>Cube Structures</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of CUBE_STRUCTURES
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Structures</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCubeModule_CubeStructures()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubeStructures'"
	 * @generated
	 */
	EList<CUBE_STRUCTURE> getCubeStructures();

	/**
	 * Returns the value of the '<em><b>Cube Structure Items</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The list of CUBE_STRUCTURE_ITEMs
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Structure Items</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getCubeModule_CubeStructureItems()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubeStructureItems'"
	 * @generated
	 */
	EList<CUBE_STRUCTURE_ITEM> getCubeStructureItems();

} // CubeModule
