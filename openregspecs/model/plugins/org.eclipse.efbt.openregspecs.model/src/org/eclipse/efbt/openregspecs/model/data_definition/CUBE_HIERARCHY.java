/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition;

import org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE HIERARCHY</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_id <em>Cube hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_type <em>Cube hierarchy type</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY()
 * @model extendedMetaData="name='CUBE_HIERARCHY' kind='empty'"
 * @generated
 */
public interface CUBE_HIERARCHY extends EObject {
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Cube hierarchy id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube hierarchy id</em>' attribute.
	 * @see #setCube_hierarchy_id(String)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Cube_hierarchy_id()
	 * @model extendedMetaData="kind='attribute' name='cube_hierarchy_id'"
	 * @generated
	 */
	String getCube_hierarchy_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_id <em>Cube hierarchy id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube hierarchy id</em>' attribute.
	 * @see #getCube_hierarchy_id()
	 * @generated
	 */
	void setCube_hierarchy_id(String value);

	/**
	 * Returns the value of the '<em><b>Cube hierarchy type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube hierarchy type</em>' attribute.
	 * @see #setCube_hierarchy_type(String)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Cube_hierarchy_type()
	 * @model extendedMetaData="kind='attribute' name='cube_hierarchy_type'"
	 * @generated
	 */
	String getCube_hierarchy_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_type <em>Cube hierarchy type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube hierarchy type</em>' attribute.
	 * @see #getCube_hierarchy_type()
	 * @generated
	 */
	void setCube_hierarchy_type(String value);

	/**
	 * Returns the value of the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Framework id</em>' reference.
	 * @see #setFramework_id(FRAMEWORK)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Framework_id()
	 * @model extendedMetaData="kind='attribute' name='framework_id'"
	 * @generated
	 */
	FRAMEWORK getFramework_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getFramework_id <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Framework id</em>' reference.
	 * @see #getFramework_id()
	 * @generated
	 */
	void setFramework_id(FRAMEWORK value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
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
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // CUBE_HIERARCHY
