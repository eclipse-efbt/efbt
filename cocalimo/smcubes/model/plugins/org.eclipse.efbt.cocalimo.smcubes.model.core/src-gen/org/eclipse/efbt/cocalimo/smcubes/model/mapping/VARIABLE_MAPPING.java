/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VARIABLE MAPPING</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVaraibleMappingItems <em>Varaible Mapping Items</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVariable_mapping_id <em>Variable mapping id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING()
 * @model
 * @generated
 */
public interface VARIABLE_MAPPING extends EObject
{
	/**
	 * Returns the value of the '<em><b>Varaible Mapping Items</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Varaible Mapping Items</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING_VaraibleMappingItems()
	 * @model containment="true"
	 * @generated
	 */
	EList<VARIABLE_MAPPING_ITEM> getVaraibleMappingItems();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getName <em>Name</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING_Code()
	 * @model
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getCode <em>Code</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING_Maintenance_agency_id()
	 * @model
	 * @generated
	 */
	String getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(String value);

	/**
	 * Returns the value of the '<em><b>Variable mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable mapping id</em>' attribute.
	 * @see #setVariable_mapping_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getVARIABLE_MAPPING_Variable_mapping_id()
	 * @model id="true"
	 * @generated
	 */
	String getVariable_mapping_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVariable_mapping_id <em>Variable mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable mapping id</em>' attribute.
	 * @see #getVariable_mapping_id()
	 * @generated
	 */
	void setVariable_mapping_id(String value);

} // VARIABLE_MAPPING
