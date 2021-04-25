/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Functionality Module Tag</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleTagImpl#getFunctionalityModule <em>Functionality Module</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FunctionalityModuleTagImpl extends TagImpl implements FunctionalityModuleTag
{
	/**
	 * The cached value of the '{@link #getFunctionalityModule() <em>Functionality Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalityModule()
	 * @generated
	 * @ordered
	 */
	protected FunctionalityModule functionalityModule;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionalityModuleTagImpl()
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
		return Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_TAG;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModule getFunctionalityModule()
	{
		if (functionalityModule != null && functionalityModule.eIsProxy()) {
			InternalEObject oldFunctionalityModule = (InternalEObject)functionalityModule;
			functionalityModule = (FunctionalityModule)eResolveProxy(oldFunctionalityModule);
			if (functionalityModule != oldFunctionalityModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE, oldFunctionalityModule, functionalityModule));
			}
		}
		return functionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionalityModule basicGetFunctionalityModule()
	{
		return functionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFunctionalityModule(FunctionalityModule newFunctionalityModule)
	{
		FunctionalityModule oldFunctionalityModule = functionalityModule;
		functionalityModule = newFunctionalityModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE, oldFunctionalityModule, functionalityModule));
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE:
				if (resolve) return getFunctionalityModule();
				return basicGetFunctionalityModule();
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE:
				setFunctionalityModule((FunctionalityModule)newValue);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE:
				setFunctionalityModule((FunctionalityModule)null);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE:
				return functionalityModule != null;
		}
		return super.eIsSet(featureID);
	}

} //FunctionalityModuleTagImpl
