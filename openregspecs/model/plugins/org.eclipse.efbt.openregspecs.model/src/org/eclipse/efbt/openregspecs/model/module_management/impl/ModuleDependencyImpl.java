/**
 */
package org.eclipse.efbt.openregspecs.model.module_management.impl;

import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleLongName;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Module Dependency</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleDependencyImpl#getModuleName <em>Module Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleDependencyImpl#getModuleVersion <em>Module Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleDependencyImpl#getTheModule <em>The Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleDependencyImpl#getLongName <em>Long Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ModuleDependencyImpl extends MinimalEObjectImpl.Container implements ModuleDependency {
	/**
	 * The default value of the '{@link #getModuleName() <em>Module Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModuleName()
	 * @generated
	 * @ordered
	 */
	protected static final String MODULE_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getModuleName() <em>Module Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModuleName()
	 * @generated
	 * @ordered
	 */
	protected String moduleName = MODULE_NAME_EDEFAULT;

	/**
	 * The default value of the '{@link #getModuleVersion() <em>Module Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModuleVersion()
	 * @generated
	 * @ordered
	 */
	protected static final String MODULE_VERSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getModuleVersion() <em>Module Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModuleVersion()
	 * @generated
	 * @ordered
	 */
	protected String moduleVersion = MODULE_VERSION_EDEFAULT;

	/**
	 * The cached value of the '{@link #getTheModule() <em>The Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTheModule()
	 * @generated
	 * @ordered
	 */
	protected org.eclipse.efbt.openregspecs.model.module_management.Module theModule;

	/**
	 * The cached value of the '{@link #getLongName() <em>Long Name</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLongName()
	 * @generated
	 * @ordered
	 */
	protected ModuleLongName longName;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ModuleDependencyImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Module_managementPackage.Literals.MODULE_DEPENDENCY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getModuleName() {
		return moduleName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setModuleName(String newModuleName) {
		String oldModuleName = moduleName;
		moduleName = newModuleName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE_DEPENDENCY__MODULE_NAME, oldModuleName, moduleName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getModuleVersion() {
		return moduleVersion;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setModuleVersion(String newModuleVersion) {
		String oldModuleVersion = moduleVersion;
		moduleVersion = newModuleVersion;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE_DEPENDENCY__MODULE_VERSION, oldModuleVersion, moduleVersion));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.openregspecs.model.module_management.Module getTheModule() {
		if (theModule != null && theModule.eIsProxy()) {
			InternalEObject oldTheModule = (InternalEObject)theModule;
			theModule = (org.eclipse.efbt.openregspecs.model.module_management.Module)eResolveProxy(oldTheModule);
			if (theModule != oldTheModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE, oldTheModule, theModule));
			}
		}
		return theModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.openregspecs.model.module_management.Module basicGetTheModule() {
		return theModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTheModule(org.eclipse.efbt.openregspecs.model.module_management.Module newTheModule) {
		org.eclipse.efbt.openregspecs.model.module_management.Module oldTheModule = theModule;
		theModule = newTheModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE, oldTheModule, theModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleLongName getLongName() {
		if (longName != null && longName.eIsProxy()) {
			InternalEObject oldLongName = (InternalEObject)longName;
			longName = (ModuleLongName)eResolveProxy(oldLongName);
			if (longName != oldLongName) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME, oldLongName, longName));
			}
		}
		return longName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleLongName basicGetLongName() {
		return longName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLongName(ModuleLongName newLongName) {
		ModuleLongName oldLongName = longName;
		longName = newLongName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME, oldLongName, longName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_NAME:
				return getModuleName();
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_VERSION:
				return getModuleVersion();
			case Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE:
				if (resolve) return getTheModule();
				return basicGetTheModule();
			case Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME:
				if (resolve) return getLongName();
				return basicGetLongName();
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
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_NAME:
				setModuleName((String)newValue);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_VERSION:
				setModuleVersion((String)newValue);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE:
				setTheModule((org.eclipse.efbt.openregspecs.model.module_management.Module)newValue);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME:
				setLongName((ModuleLongName)newValue);
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
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_NAME:
				setModuleName(MODULE_NAME_EDEFAULT);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_VERSION:
				setModuleVersion(MODULE_VERSION_EDEFAULT);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE:
				setTheModule((org.eclipse.efbt.openregspecs.model.module_management.Module)null);
				return;
			case Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME:
				setLongName((ModuleLongName)null);
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
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_NAME:
				return MODULE_NAME_EDEFAULT == null ? moduleName != null : !MODULE_NAME_EDEFAULT.equals(moduleName);
			case Module_managementPackage.MODULE_DEPENDENCY__MODULE_VERSION:
				return MODULE_VERSION_EDEFAULT == null ? moduleVersion != null : !MODULE_VERSION_EDEFAULT.equals(moduleVersion);
			case Module_managementPackage.MODULE_DEPENDENCY__THE_MODULE:
				return theModule != null;
			case Module_managementPackage.MODULE_DEPENDENCY__LONG_NAME:
				return longName != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (moduleName: ");
		result.append(moduleName);
		result.append(", moduleVersion: ");
		result.append(moduleVersion);
		result.append(')');
		return result.toString();
	}

} //ModuleDependencyImpl
