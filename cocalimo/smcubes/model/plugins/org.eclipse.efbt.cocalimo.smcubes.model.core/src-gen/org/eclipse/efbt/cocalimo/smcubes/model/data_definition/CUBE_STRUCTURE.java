/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE STRUCTURE</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCube_structure_id <em>Cube structure id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE()
 * @model extendedMetaData="name='CUBE_STRUCTURE' kind='empty'"
 * @generated
 */
public interface CUBE_STRUCTURE extends EObject
{
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Cube structure id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube structure id</em>' attribute.
	 * @see #setCube_structure_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Cube_structure_id()
	 * @model extendedMetaData="kind='attribute' name='cube_structure_id'"
	 * @generated
	 */
	String getCube_structure_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCube_structure_id <em>Cube structure id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube structure id</em>' attribute.
	 * @see #getCube_structure_id()
	 * @generated
	 */
	void setCube_structure_id(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_from <em>Valid from</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_to <em>Valid to</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_Version()
	 * @model extendedMetaData="kind='attribute' name='version'"
	 * @generated
	 */
	String getVersion();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getVersion <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Version</em>' attribute.
	 * @see #getVersion()
	 * @generated
	 */
	void setVersion(String value);

	/**
	 * Returns the value of the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Display Name</em>' attribute.
	 * @see #setDisplayName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_DisplayName()
	 * @model
	 * @generated
	 */
	String getDisplayName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDisplayName <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Display Name</em>' attribute.
	 * @see #getDisplayName()
	 * @generated
	 */
	void setDisplayName(String value);

} // CUBE_STRUCTURE
