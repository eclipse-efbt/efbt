/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE MAPPING</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCube_mapping_id <em>Cube mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getSource_cube <em>Source cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getTarget_cube <em>Target cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMappingsToCube <em>Mappings To Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING()
 * @model
 * @generated
 */
public interface CUBE_MAPPING extends EObject {
	/**
	 * Returns the value of the '<em><b>Cube mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube mapping id</em>' attribute.
	 * @see #setCube_mapping_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Cube_mapping_id()
	 * @model id="true"
	 * @generated
	 */
	String getCube_mapping_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCube_mapping_id <em>Cube mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube mapping id</em>' attribute.
	 * @see #getCube_mapping_id()
	 * @generated
	 */
	void setCube_mapping_id(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Description()
	 * @model
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Source cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source cube</em>' reference.
	 * @see #setSource_cube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Source_cube()
	 * @model
	 * @generated
	 */
	CUBE getSource_cube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getSource_cube <em>Source cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source cube</em>' reference.
	 * @see #getSource_cube()
	 * @generated
	 */
	void setSource_cube(CUBE value);

	/**
	 * Returns the value of the '<em><b>Target cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target cube</em>' reference.
	 * @see #setTarget_cube(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Target_cube()
	 * @model
	 * @generated
	 */
	CUBE getTarget_cube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getTarget_cube <em>Target cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target cube</em>' reference.
	 * @see #getTarget_cube()
	 * @generated
	 */
	void setTarget_cube(CUBE value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Code()
	 * @model
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #setMaintenance_agency_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_Maintenance_agency_id()
	 * @model
	 * @generated
	 */
	String getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(String value);

	/**
	 * Returns the value of the '<em><b>Mappings To Cube</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mappings To Cube</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getCUBE_MAPPING_MappingsToCube()
	 * @model containment="true"
	 * @generated
	 */
	EList<MAPPING_TO_CUBE> getMappingsToCube();

} // CUBE_MAPPING
