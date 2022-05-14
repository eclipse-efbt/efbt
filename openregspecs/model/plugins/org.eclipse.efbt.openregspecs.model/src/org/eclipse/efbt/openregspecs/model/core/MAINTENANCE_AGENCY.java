/**
 */
package org.eclipse.efbt.openregspecs.model.core;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MAINTENANCE AGENCY</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getMAINTENANCE_AGENCY()
 * @model extendedMetaData="name='MAINTENANCE_AGENCY' kind='empty'"
 * @generated
 */
public interface MAINTENANCE_AGENCY extends EObject {
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getMAINTENANCE_AGENCY_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getCode <em>Code</em>}' attribute.
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
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getMAINTENANCE_AGENCY_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	String getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getMaintenance_agency_id <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage#getMAINTENANCE_AGENCY_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // MAINTENANCE_AGENCY
