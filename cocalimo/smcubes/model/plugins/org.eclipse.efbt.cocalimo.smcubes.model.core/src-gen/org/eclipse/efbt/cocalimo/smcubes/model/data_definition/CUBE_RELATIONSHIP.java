/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE RELATIONSHIP</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCube_relationship_id <em>Cube relationship id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#isEstablishes_integrity <em>Establishes integrity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_id <em>Foreign cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_variable_code <em>Foreign cube variable code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_id <em>Primary cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_variable_id <em>Primary cube variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getVersion <em>Version</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP()
 * @model extendedMetaData="name='CUBE_RELATIONSHIP' kind='empty'"
 * @generated
 */
public interface CUBE_RELATIONSHIP extends EObject
{
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Cube relationship id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube relationship id</em>' attribute.
	 * @see #setCube_relationship_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Cube_relationship_id()
	 * @model extendedMetaData="kind='attribute' name='cube_relationship_id'"
	 * @generated
	 */
	String getCube_relationship_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCube_relationship_id <em>Cube relationship id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube relationship id</em>' attribute.
	 * @see #getCube_relationship_id()
	 * @generated
	 */
	void setCube_relationship_id(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Establishes integrity</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Establishes integrity</em>' attribute.
	 * @see #setEstablishes_integrity(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Establishes_integrity()
	 * @model extendedMetaData="kind='attribute' name='establishes_integrity'"
	 * @generated
	 */
	boolean isEstablishes_integrity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#isEstablishes_integrity <em>Establishes integrity</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Establishes integrity</em>' attribute.
	 * @see #isEstablishes_integrity()
	 * @generated
	 */
	void setEstablishes_integrity(boolean value);

	/**
	 * Returns the value of the '<em><b>Foreign cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Foreign cube id</em>' reference.
	 * @see #setForeign_cube_id(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Foreign_cube_id()
	 * @model extendedMetaData="kind='attribute' name='foreign_cube_id'"
	 * @generated
	 */
	CUBE getForeign_cube_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_id <em>Foreign cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Foreign cube id</em>' reference.
	 * @see #getForeign_cube_id()
	 * @generated
	 */
	void setForeign_cube_id(CUBE value);

	/**
	 * Returns the value of the '<em><b>Foreign cube variable code</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Foreign cube variable code</em>' reference.
	 * @see #setForeign_cube_variable_code(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Foreign_cube_variable_code()
	 * @model extendedMetaData="kind='attribute' name='foreign_cube_variable_code'"
	 * @generated
	 */
	VARIABLE getForeign_cube_variable_code();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_variable_code <em>Foreign cube variable code</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Foreign cube variable code</em>' reference.
	 * @see #getForeign_cube_variable_code()
	 * @generated
	 */
	void setForeign_cube_variable_code(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' reference.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(MAINTENANCE_AGENCY value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Primary cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Primary cube id</em>' reference.
	 * @see #setPrimary_cube_id(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Primary_cube_id()
	 * @model extendedMetaData="kind='attribute' name='primary_cube_id'"
	 * @generated
	 */
	CUBE getPrimary_cube_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_id <em>Primary cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Primary cube id</em>' reference.
	 * @see #getPrimary_cube_id()
	 * @generated
	 */
	void setPrimary_cube_id(CUBE value);

	/**
	 * Returns the value of the '<em><b>Primary cube variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Primary cube variable id</em>' reference.
	 * @see #setPrimary_cube_variable_id(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Primary_cube_variable_id()
	 * @model extendedMetaData="kind='attribute' name='primary_cube_variable_id'"
	 * @generated
	 */
	VARIABLE getPrimary_cube_variable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_variable_id <em>Primary cube variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Primary cube variable id</em>' reference.
	 * @see #getPrimary_cube_variable_id()
	 * @generated
	 */
	void setPrimary_cube_variable_id(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_from <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid from</em>' attribute.
	 * @see #getValid_from()
	 * @generated
	 */
	void setValid_from(Date value);

	/**
	 * Returns the value of the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid to</em>' attribute.
	 * @see #setValid_to(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

	/**
	 * Returns the value of the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Version</em>' attribute.
	 * @see #setVersion(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_RELATIONSHIP_Version()
	 * @model extendedMetaData="kind='attribute' name='version'"
	 * @generated
	 */
	String getVersion();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getVersion <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Version</em>' attribute.
	 * @see #getVersion()
	 * @generated
	 */
	void setVersion(String value);

} // CUBE_RELATIONSHIP
