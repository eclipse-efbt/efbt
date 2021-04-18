/**
 */
package org.eclipse.efbt.controller.core.model.platform_call;

import org.eclipse.efbt.language.dtm.model.transformation.dtmProgram;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Program And Calls</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getPlatformCalls <em>Platform Calls</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getProgram <em>Program</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getProgramAndCalls()
 * @model
 * @generated
 */
public interface ProgramAndCalls extends EObject {
	/**
	 * Returns the value of the '<em><b>Platform Calls</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Platform Calls</em>' containment reference.
	 * @see #setPlatformCalls(PlatformCallModule)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getProgramAndCalls_PlatformCalls()
	 * @model containment="true"
	 * @generated
	 */
	PlatformCallModule getPlatformCalls();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getPlatformCalls <em>Platform Calls</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Platform Calls</em>' containment reference.
	 * @see #getPlatformCalls()
	 * @generated
	 */
	void setPlatformCalls(PlatformCallModule value);

	/**
	 * Returns the value of the '<em><b>Program</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Program</em>' containment reference.
	 * @see #setProgram(dtmProgram)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getProgramAndCalls_Program()
	 * @model containment="true"
	 * @generated
	 */
	dtmProgram getProgram();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getProgram <em>Program</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Program</em>' containment reference.
	 * @see #getProgram()
	 * @generated
	 */
	void setProgram(dtmProgram value);

} // ProgramAndCalls
