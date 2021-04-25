/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Functionality Module Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl#getFunctionalityModules <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl#getFunctionalityModuleTags <em>Functionality Module Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl#getSceanrioTags <em>Sceanrio Tags</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FunctionalityModuleModuleImpl extends ModuleImpl implements FunctionalityModuleModule
{
	/**
	 * The cached value of the '{@link #getFunctionalityModules() <em>Functionality Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalityModules()
	 * @generated
	 * @ordered
	 */
	protected EList<FunctionalityModule> functionalityModules;

	/**
	 * The cached value of the '{@link #getFunctionalityModuleTags() <em>Functionality Module Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalityModuleTags()
	 * @generated
	 * @ordered
	 */
	protected EList<FunctionalityModuleTag> functionalityModuleTags;

	/**
	 * The cached value of the '{@link #getSceanrioTags() <em>Sceanrio Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSceanrioTags()
	 * @generated
	 * @ordered
	 */
	protected EList<ScenarioTag> sceanrioTags;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionalityModuleModuleImpl()
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
		return Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FunctionalityModule> getFunctionalityModules()
	{
		if (functionalityModules == null) {
			functionalityModules = new EObjectContainmentEList<FunctionalityModule>(FunctionalityModule.class, this, Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES);
		}
		return functionalityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FunctionalityModuleTag> getFunctionalityModuleTags()
	{
		if (functionalityModuleTags == null) {
			functionalityModuleTags = new EObjectContainmentEList<FunctionalityModuleTag>(FunctionalityModuleTag.class, this, Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS);
		}
		return functionalityModuleTags;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ScenarioTag> getSceanrioTags()
	{
		if (sceanrioTags == null) {
			sceanrioTags = new EObjectContainmentEList<ScenarioTag>(ScenarioTag.class, this, Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS);
		}
		return sceanrioTags;
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
				return ((InternalEList<?>)getFunctionalityModules()).basicRemove(otherEnd, msgs);
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
				return ((InternalEList<?>)getFunctionalityModuleTags()).basicRemove(otherEnd, msgs);
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
				return ((InternalEList<?>)getSceanrioTags()).basicRemove(otherEnd, msgs);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
				return getFunctionalityModules();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
				return getFunctionalityModuleTags();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
				return getSceanrioTags();
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
				getFunctionalityModules().clear();
				getFunctionalityModules().addAll((Collection<? extends FunctionalityModule>)newValue);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
				getFunctionalityModuleTags().clear();
				getFunctionalityModuleTags().addAll((Collection<? extends FunctionalityModuleTag>)newValue);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
				getSceanrioTags().clear();
				getSceanrioTags().addAll((Collection<? extends ScenarioTag>)newValue);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
				getFunctionalityModules().clear();
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
				getFunctionalityModuleTags().clear();
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
				getSceanrioTags().clear();
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
				return functionalityModules != null && !functionalityModules.isEmpty();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
				return functionalityModuleTags != null && !functionalityModuleTags.isEmpty();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
				return sceanrioTags != null && !sceanrioTags.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FunctionalityModuleModuleImpl
