/**
 */
package org.eclipse.efbt.openregspecs.model.core;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>DOMAIN</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getData_type <em>Data type</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#isIs_enumerated <em>Is enumerated</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#isIs_reference <em>Is reference</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN()
 * @model extendedMetaData="name='DOMAIN' kind='empty'"
 * @generated
 */
public interface DOMAIN extends EObject {
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Data type</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.openregspecs.model.core.FACET_VALUE_TYPE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Data type</em>' attribute.
	 * @see org.eclipse.efbt.openregspecs.model.core.FACET_VALUE_TYPE
	 * @see #setData_type(FACET_VALUE_TYPE)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Data_type()
	 * @model extendedMetaData="kind='attribute' name='data_type'"
	 * @generated
	 */
	FACET_VALUE_TYPE getData_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getData_type <em>Data type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Data type</em>' attribute.
	 * @see org.eclipse.efbt.openregspecs.model.core.FACET_VALUE_TYPE
	 * @see #getData_type()
	 * @generated
	 */
	void setData_type(FACET_VALUE_TYPE value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Domain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Domain id</em>' attribute.
	 * @see #setDomain_id(String)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Domain_id()
	 * @model extendedMetaData="kind='attribute' name='domain_id'"
	 * @generated
	 */
	String getDomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDomain_id <em>Domain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Domain id</em>' attribute.
	 * @see #getDomain_id()
	 * @generated
	 */
	void setDomain_id(String value);

	/**
	 * Returns the value of the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet id</em>' reference.
	 * @see #setFacet_id(FACET_COLLECTION)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Facet_id()
	 * @model extendedMetaData="kind='attribute' name='facet_id'"
	 * @generated
	 */
	FACET_COLLECTION getFacet_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getFacet_id <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet id</em>' reference.
	 * @see #getFacet_id()
	 * @generated
	 */
	void setFacet_id(FACET_COLLECTION value);

	/**
	 * Returns the value of the '<em><b>Is enumerated</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is enumerated</em>' attribute.
	 * @see #setIs_enumerated(boolean)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Is_enumerated()
	 * @model extendedMetaData="kind='attribute' name='is_enumerated'"
	 * @generated
	 */
	boolean isIs_enumerated();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#isIs_enumerated <em>Is enumerated</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is enumerated</em>' attribute.
	 * @see #isIs_enumerated()
	 * @generated
	 */
	void setIs_enumerated(boolean value);

	/**
	 * Returns the value of the '<em><b>Is reference</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is reference</em>' attribute.
	 * @see #setIs_reference(boolean)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Is_reference()
	 * @model extendedMetaData="kind='attribute' name='is_reference'"
	 * @generated
	 */
	boolean isIs_reference();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#isIs_reference <em>Is reference</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is reference</em>' attribute.
	 * @see #isIs_reference()
	 * @generated
	 */
	void setIs_reference(boolean value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
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
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getName <em>Name</em>}' attribute.
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
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getDOMAIN_DisplayName()
	 * @model
	 * @generated
	 */
	String getDisplayName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN#getDisplayName <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Display Name</em>' attribute.
	 * @see #getDisplayName()
	 * @generated
	 */
	void setDisplayName(String value);

} // DOMAIN
