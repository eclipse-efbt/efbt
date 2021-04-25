/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls;

import org.eclipse.efbt.language.dtm.model.transformation.dtmProgram;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Program And Calls</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl#getPlatformCalls <em>Platform Calls</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl#getProgram <em>Program</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ProgramAndCallsImpl extends MinimalEObjectImpl.Container implements ProgramAndCalls {
	/**
	 * The cached value of the '{@link #getPlatformCalls() <em>Platform Calls</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPlatformCalls()
	 * @generated
	 * @ordered
	 */
	protected PlatformCallModule platformCalls;

	/**
	 * The cached value of the '{@link #getProgram() <em>Program</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getProgram()
	 * @generated
	 * @ordered
	 */
	protected dtmProgram program;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ProgramAndCallsImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Platform_callPackage.Literals.PROGRAM_AND_CALLS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCallModule getPlatformCalls() {
		return platformCalls;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetPlatformCalls(PlatformCallModule newPlatformCalls, NotificationChain msgs) {
		PlatformCallModule oldPlatformCalls = platformCalls;
		platformCalls = newPlatformCalls;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS, oldPlatformCalls, newPlatformCalls);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPlatformCalls(PlatformCallModule newPlatformCalls) {
		if (newPlatformCalls != platformCalls) {
			NotificationChain msgs = null;
			if (platformCalls != null)
				msgs = ((InternalEObject)platformCalls).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS, null, msgs);
			if (newPlatformCalls != null)
				msgs = ((InternalEObject)newPlatformCalls).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS, null, msgs);
			msgs = basicSetPlatformCalls(newPlatformCalls, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS, newPlatformCalls, newPlatformCalls));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public dtmProgram getProgram() {
		return program;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetProgram(dtmProgram newProgram, NotificationChain msgs) {
		dtmProgram oldProgram = program;
		program = newProgram;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM, oldProgram, newProgram);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setProgram(dtmProgram newProgram) {
		if (newProgram != program) {
			NotificationChain msgs = null;
			if (program != null)
				msgs = ((InternalEObject)program).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM, null, msgs);
			if (newProgram != null)
				msgs = ((InternalEObject)newProgram).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM, null, msgs);
			msgs = basicSetProgram(newProgram, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM, newProgram, newProgram));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS:
				return basicSetPlatformCalls(null, msgs);
			case Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM:
				return basicSetProgram(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS:
				return getPlatformCalls();
			case Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM:
				return getProgram();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS:
				setPlatformCalls((PlatformCallModule)newValue);
				return;
			case Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM:
				setProgram((dtmProgram)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS:
				setPlatformCalls((PlatformCallModule)null);
				return;
			case Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM:
				setProgram((dtmProgram)null);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Platform_callPackage.PROGRAM_AND_CALLS__PLATFORM_CALLS:
				return platformCalls != null;
			case Platform_callPackage.PROGRAM_AND_CALLS__PROGRAM:
				return program != null;
		}
		return super.eIsSet(featureID);
	}

} //ProgramAndCallsImpl
