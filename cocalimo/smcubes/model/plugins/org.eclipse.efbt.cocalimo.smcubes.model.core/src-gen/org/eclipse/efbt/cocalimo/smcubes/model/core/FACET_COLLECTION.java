/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>FACET COLLECTION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_value_type <em>Facet value type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION()
 * @model extendedMetaData="name='FACET_COLLECTION' kind='empty'"
 * @generated
 */
public interface FACET_COLLECTION extends EObject
{
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Facet id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet id</em>' attribute.
	 * @see #setFacet_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION_Facet_id()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='facet_id'"
	 * @generated
	 */
	String getFacet_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_id <em>Facet id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet id</em>' attribute.
	 * @see #getFacet_id()
	 * @generated
	 */
	void setFacet_id(String value);

	/**
	 * Returns the value of the '<em><b>Facet value type</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet value type</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @see #setFacet_value_type(FACET_VALUE_TYPE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION_Facet_value_type()
	 * @model extendedMetaData="kind='attribute' name='facet_value_type'"
	 * @generated
	 */
	FACET_VALUE_TYPE getFacet_value_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_value_type <em>Facet value type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet value type</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @see #getFacet_value_type()
	 * @generated
	 */
	void setFacet_value_type(FACET_VALUE_TYPE value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_COLLECTION_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // FACET_COLLECTION
