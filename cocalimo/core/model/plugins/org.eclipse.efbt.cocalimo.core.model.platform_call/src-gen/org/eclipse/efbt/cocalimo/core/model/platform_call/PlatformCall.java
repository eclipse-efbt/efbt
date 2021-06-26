/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Platform Call</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  A Platform Call 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getErrorMessage <em>Error Message</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getReturnStatus <em>Return Status</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getPlatformCall()
 * @model extendedMetaData="name='PlatformCall' kind='empty'"
 * @generated
 */
public interface PlatformCall extends EObject {
	/**
	 * Returns the value of the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Any error message returned by the platform call
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Error Message</em>' attribute.
	 * @see #setErrorMessage(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getPlatformCall_ErrorMessage()
	 * @model extendedMetaData="kind='attribute' name='errorMessage'"
	 * @generated
	 */
	String getErrorMessage();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getErrorMessage <em>Error Message</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Error Message</em>' attribute.
	 * @see #getErrorMessage()
	 * @generated
	 */
	void setErrorMessage(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the platform call
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getPlatformCall_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The return status of the platform call, 0 represents a sucessful execution.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Return Status</em>' attribute.
	 * @see #setReturnStatus(int)
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#getPlatformCall_ReturnStatus()
	 * @model extendedMetaData="kind='attribute' name='returnStatus'"
	 * @generated
	 */
	int getReturnStatus();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getReturnStatus <em>Return Status</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Return Status</em>' attribute.
	 * @see #getReturnStatus()
	 * @generated
	 */
	void setReturnStatus(int value);

} // PlatformCall
