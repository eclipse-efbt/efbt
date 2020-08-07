/**
 */
package org.eclipse.efbt.controller.model.platform_call;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Platform Call Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module of PlatfromCalls
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.PlatformCallModule#getPlatformCalls <em>Platform Calls</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getPlatformCallModule()
 * @model extendedMetaData="name='PlatformCallModule' kind='elementOnly'"
 * @generated
 */
public interface PlatformCallModule extends org.eclipse.efbt.aorta.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Platform Calls</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.controller.model.platform_call.PlatformCall}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The group of PlatformCalls
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Platform Calls</em>' containment reference list.
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getPlatformCallModule_PlatformCalls()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='platformCalls'"
	 * @generated
	 */
	EList<PlatformCall> getPlatformCalls();

} // PlatformCallModule
