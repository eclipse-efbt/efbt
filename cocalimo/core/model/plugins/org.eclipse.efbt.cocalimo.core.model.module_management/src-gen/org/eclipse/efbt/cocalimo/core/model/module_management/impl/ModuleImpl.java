/**
 */
package org.eclipse.efbt.cocalimo.core.model.module_management.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies;
import org.eclipse.efbt.cocalimo.core.model.module_management.ModuleLongName;
import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getDependencies <em>Dependencies</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getTheDescription <em>The Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getLicense <em>License</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl#getLongName <em>Long Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class ModuleImpl extends MinimalEObjectImpl.Container implements org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * The cached value of the '{@link #getDependencies() <em>Dependencies</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDependencies()
	 * @generated
	 * @ordered
	 */
	protected ModuleDependencies dependencies;

	/**
	 * The default value of the '{@link #getTheDescription() <em>The Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTheDescription()
	 * @generated
	 * @ordered
	 */
	protected static final String THE_DESCRIPTION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTheDescription() <em>The Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTheDescription()
	 * @generated
	 * @ordered
	 */
	protected String theDescription = THE_DESCRIPTION_EDEFAULT;

	/**
	 * The default value of the '{@link #getLicense() <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLicense()
	 * @generated
	 * @ordered
	 */
	protected static final String LICENSE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getLicense() <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLicense()
	 * @generated
	 * @ordered
	 */
	protected String license = LICENSE_EDEFAULT;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The default value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected static final String VERSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected String version = VERSION_EDEFAULT;

	/**
	 * The cached value of the '{@link #getLongName() <em>Long Name</em>}' containment reference.
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
	protected ModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Module_managementPackage.Literals.MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ModuleDependencies getDependencies()
	{
		return dependencies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetDependencies(ModuleDependencies newDependencies, NotificationChain msgs)
	{
		ModuleDependencies oldDependencies = dependencies;
		dependencies = newDependencies;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__DEPENDENCIES, oldDependencies, newDependencies);
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
	public void setDependencies(ModuleDependencies newDependencies)
	{
		if (newDependencies != dependencies) {
			NotificationChain msgs = null;
			if (dependencies != null)
				msgs = ((InternalEObject)dependencies).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Module_managementPackage.MODULE__DEPENDENCIES, null, msgs);
			if (newDependencies != null)
				msgs = ((InternalEObject)newDependencies).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Module_managementPackage.MODULE__DEPENDENCIES, null, msgs);
			msgs = basicSetDependencies(newDependencies, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__DEPENDENCIES, newDependencies, newDependencies));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getTheDescription()
	{
		return theDescription;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTheDescription(String newTheDescription)
	{
		String oldTheDescription = theDescription;
		theDescription = newTheDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__THE_DESCRIPTION, oldTheDescription, theDescription));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getLicense()
	{
		return license;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLicense(String newLicense)
	{
		String oldLicense = license;
		license = newLicense;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__LICENSE, oldLicense, license));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getVersion()
	{
		return version;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVersion(String newVersion)
	{
		String oldVersion = version;
		version = newVersion;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__VERSION, oldVersion, version));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ModuleLongName getLongName()
	{
		return longName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetLongName(ModuleLongName newLongName, NotificationChain msgs)
	{
		ModuleLongName oldLongName = longName;
		longName = newLongName;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__LONG_NAME, oldLongName, newLongName);
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
	public void setLongName(ModuleLongName newLongName)
	{
		if (newLongName != longName) {
			NotificationChain msgs = null;
			if (longName != null)
				msgs = ((InternalEObject)longName).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Module_managementPackage.MODULE__LONG_NAME, null, msgs);
			if (newLongName != null)
				msgs = ((InternalEObject)newLongName).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Module_managementPackage.MODULE__LONG_NAME, null, msgs);
			msgs = basicSetLongName(newLongName, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Module_managementPackage.MODULE__LONG_NAME, newLongName, newLongName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Module_managementPackage.MODULE__DEPENDENCIES:
				return basicSetDependencies(null, msgs);
			case Module_managementPackage.MODULE__LONG_NAME:
				return basicSetLongName(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Module_managementPackage.MODULE__DEPENDENCIES:
				return getDependencies();
			case Module_managementPackage.MODULE__THE_DESCRIPTION:
				return getTheDescription();
			case Module_managementPackage.MODULE__LICENSE:
				return getLicense();
			case Module_managementPackage.MODULE__NAME:
				return getName();
			case Module_managementPackage.MODULE__VERSION:
				return getVersion();
			case Module_managementPackage.MODULE__LONG_NAME:
				return getLongName();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Module_managementPackage.MODULE__DEPENDENCIES:
				setDependencies((ModuleDependencies)newValue);
				return;
			case Module_managementPackage.MODULE__THE_DESCRIPTION:
				setTheDescription((String)newValue);
				return;
			case Module_managementPackage.MODULE__LICENSE:
				setLicense((String)newValue);
				return;
			case Module_managementPackage.MODULE__NAME:
				setName((String)newValue);
				return;
			case Module_managementPackage.MODULE__VERSION:
				setVersion((String)newValue);
				return;
			case Module_managementPackage.MODULE__LONG_NAME:
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Module_managementPackage.MODULE__DEPENDENCIES:
				setDependencies((ModuleDependencies)null);
				return;
			case Module_managementPackage.MODULE__THE_DESCRIPTION:
				setTheDescription(THE_DESCRIPTION_EDEFAULT);
				return;
			case Module_managementPackage.MODULE__LICENSE:
				setLicense(LICENSE_EDEFAULT);
				return;
			case Module_managementPackage.MODULE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Module_managementPackage.MODULE__VERSION:
				setVersion(VERSION_EDEFAULT);
				return;
			case Module_managementPackage.MODULE__LONG_NAME:
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Module_managementPackage.MODULE__DEPENDENCIES:
				return dependencies != null;
			case Module_managementPackage.MODULE__THE_DESCRIPTION:
				return THE_DESCRIPTION_EDEFAULT == null ? theDescription != null : !THE_DESCRIPTION_EDEFAULT.equals(theDescription);
			case Module_managementPackage.MODULE__LICENSE:
				return LICENSE_EDEFAULT == null ? license != null : !LICENSE_EDEFAULT.equals(license);
			case Module_managementPackage.MODULE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Module_managementPackage.MODULE__VERSION:
				return VERSION_EDEFAULT == null ? version != null : !VERSION_EDEFAULT.equals(version);
			case Module_managementPackage.MODULE__LONG_NAME:
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
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (theDescription: ");
		result.append(theDescription);
		result.append(", license: ");
		result.append(license);
		result.append(", name: ");
		result.append(name);
		result.append(", version: ");
		result.append(version);
		result.append(')');
		return result.toString();
	}

} //ModuleImpl
