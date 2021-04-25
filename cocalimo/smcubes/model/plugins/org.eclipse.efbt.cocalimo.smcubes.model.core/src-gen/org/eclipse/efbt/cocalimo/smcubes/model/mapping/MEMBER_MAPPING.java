/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MEMBER MAPPING</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMember_mapping_id <em>Member mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMemberMappingItems <em>Member Mapping Items</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING()
 * @model
 * @generated
 */
public interface MEMBER_MAPPING extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getName <em>Name</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING_Code()
	 * @model
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getCode <em>Code</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING_Maintenance_agency_id()
	 * @model
	 * @generated
	 */
	String getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(String value);

	/**
	 * Returns the value of the '<em><b>Member mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member mapping id</em>' attribute.
	 * @see #setMember_mapping_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING_Member_mapping_id()
	 * @model id="true"
	 * @generated
	 */
	String getMember_mapping_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMember_mapping_id <em>Member mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member mapping id</em>' attribute.
	 * @see #getMember_mapping_id()
	 * @generated
	 */
	void setMember_mapping_id(String value);

	/**
	 * Returns the value of the '<em><b>Member Mapping Items</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member Mapping Items</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMEMBER_MAPPING_MemberMappingItems()
	 * @model containment="true"
	 * @generated
	 */
	EList<MEMBER_MAPPING_ITEM> getMemberMappingItems();

} // MEMBER_MAPPING
