/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MappingDefinitionModule;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Smcubes Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This class stores the reference to the Modules which contain the SMCubes items
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getDomains <em>Domains</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMembers <em>Members</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getVariables <em>Variables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCubes <em>Cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCombinations <em>Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getFunctionalModules <em>Functional Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMappings <em>Mappings</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel()
 * @model
 * @generated
 */
public interface SmcubesModel extends EObject
{
	/**
	 * Returns the value of the '<em><b>Domains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Domains
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Domains</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Domains()
	 * @model containment="true"
	 * @generated
	 */
	EList<DomainModule> getDomains();

	/**
	 * Returns the value of the '<em><b>Members</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Members
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Members</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Members()
	 * @model containment="true"
	 * @generated
	 */
	EList<MemberModule> getMembers();

	/**
	 * Returns the value of the '<em><b>Variables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Variables
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variables</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Variables()
	 * @model containment="true"
	 * @generated
	 */
	EList<VariableModule> getVariables();

	/**
	 * Returns the value of the '<em><b>Cubes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Cubes, The Cube Structures and the Cube Structure Items.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cubes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Cubes()
	 * @model containment="true"
	 * @generated
	 */
	EList<CubeModule> getCubes();

	/**
	 * Returns the value of the '<em><b>Combinations</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Combinations
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Combinations</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Combinations()
	 * @model containment="true"
	 * @generated
	 */
	EList<CombinationModule> getCombinations();

	/**
	 * Returns the value of the '<em><b>Functional Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Tranformation Schemes and contained Transformations
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functional Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_FunctionalModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<TransformationSchemeModule> getFunctionalModules();

	/**
	 * Returns the value of the '<em><b>Mappings</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MappingDefinitionModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Mappings
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Mappings</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Mappings()
	 * @model containment="true"
	 * @generated
	 */
	EList<MappingDefinitionModule> getMappings();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the SMCubesModel
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage#getSmcubesModel_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // SmcubesModel
