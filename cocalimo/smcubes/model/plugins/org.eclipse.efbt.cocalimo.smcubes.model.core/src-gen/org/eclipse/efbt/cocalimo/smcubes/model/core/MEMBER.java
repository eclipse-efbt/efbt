/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MEMBER</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER()
 * @model extendedMetaData="name='MEMBER' kind='empty'"
 * @generated
 */
public interface MEMBER extends EObject
{
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Domain id</em>' reference.
	 * @see #setDomain_id(DOMAIN)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Domain_id()
	 * @model extendedMetaData="kind='attribute' name='domain_id'"
	 * @generated
	 */
	DOMAIN getDomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDomain_id <em>Domain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Domain id</em>' reference.
	 * @see #getDomain_id()
	 * @generated
	 */
	void setDomain_id(DOMAIN value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' reference.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(MAINTENANCE_AGENCY value);

	/**
	 * Returns the value of the '<em><b>Member id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' attribute.
	 * @see #setMember_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Member_id()
	 * @model
	 * @generated
	 */
	String getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMember_id <em>Member id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' attribute.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Display Name</em>' attribute.
	 * @see #setDisplayName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_DisplayName()
	 * @model
	 * @generated
	 */
	String getDisplayName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDisplayName <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Display Name</em>' attribute.
	 * @see #getDisplayName()
	 * @generated
	 */
	void setDisplayName(String value);

} // MEMBER
