/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Functionality Module Set</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleSetImpl#getSubModules <em>Sub Modules</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FunctionalityModuleSetImpl extends FunctionalityModuleImpl implements FunctionalityModuleSet
{
	/**
	 * The cached value of the '{@link #getSubModules() <em>Sub Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubModules()
	 * @generated
	 * @ordered
	 */
	protected EList<FunctionalityModule> subModules;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionalityModuleSetImpl()
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
		return Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FunctionalityModule> getSubModules()
	{
		if (subModules == null) {
			subModules = new EObjectContainmentEList<FunctionalityModule>(FunctionalityModule.class, this, Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES);
		}
		return subModules;
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES:
				return ((InternalEList<?>)getSubModules()).basicRemove(otherEnd, msgs);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES:
				return getSubModules();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES:
				getSubModules().clear();
				getSubModules().addAll((Collection<? extends FunctionalityModule>)newValue);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES:
				getSubModules().clear();
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET__SUB_MODULES:
				return subModules != null && !subModules.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FunctionalityModuleSetImpl
