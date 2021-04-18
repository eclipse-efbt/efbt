/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl#getUsedModule <em>Used Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class FunctionalityModuleImpl extends MinimalEObjectImpl.Container implements FunctionalityModule
{
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
	 * The cached value of the '{@link #getUsedModule() <em>Used Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUsedModule()
	 * @generated
	 * @ordered
	 */
	protected FunctionalityModule usedModule;

	/**
	 * The cached value of the '{@link #getScenario() <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenario()
	 * @generated
	 * @ordered
	 */
	protected Scenario scenario;

	/**
	 * The cached value of the '{@link #getScenarioSet() <em>Scenario Set</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioSet()
	 * @generated
	 * @ordered
	 */
	protected ScenarioSet scenarioSet;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionalityModuleImpl()
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
		return Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModule getUsedModule()
	{
		if (usedModule != null && usedModule.eIsProxy()) {
			InternalEObject oldUsedModule = (InternalEObject)usedModule;
			usedModule = (FunctionalityModule)eResolveProxy(oldUsedModule);
			if (usedModule != oldUsedModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE, oldUsedModule, usedModule));
			}
		}
		return usedModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionalityModule basicGetUsedModule()
	{
		return usedModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUsedModule(FunctionalityModule newUsedModule)
	{
		FunctionalityModule oldUsedModule = usedModule;
		usedModule = newUsedModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE, oldUsedModule, usedModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario getScenario()
	{
		if (scenario != null && scenario.eIsProxy()) {
			InternalEObject oldScenario = (InternalEObject)scenario;
			scenario = (Scenario)eResolveProxy(oldScenario);
			if (scenario != oldScenario) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO, oldScenario, scenario));
			}
		}
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario basicGetScenario()
	{
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScenario(Scenario newScenario)
	{
		Scenario oldScenario = scenario;
		scenario = newScenario;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO, oldScenario, scenario));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet getScenarioSet()
	{
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetScenarioSet(ScenarioSet newScenarioSet, NotificationChain msgs)
	{
		ScenarioSet oldScenarioSet = scenarioSet;
		scenarioSet = newScenarioSet;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET, oldScenarioSet, newScenarioSet);
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
	public void setScenarioSet(ScenarioSet newScenarioSet)
	{
		if (newScenarioSet != scenarioSet) {
			NotificationChain msgs = null;
			if (scenarioSet != null)
				msgs = ((InternalEObject)scenarioSet).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET, null, msgs);
			if (newScenarioSet != null)
				msgs = ((InternalEObject)newScenarioSet).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET, null, msgs);
			msgs = basicSetScenarioSet(newScenarioSet, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET, newScenarioSet, newScenarioSet));
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET:
				return basicSetScenarioSet(null, msgs);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__NAME:
				return getName();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE:
				if (resolve) return getUsedModule();
				return basicGetUsedModule();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO:
				if (resolve) return getScenario();
				return basicGetScenario();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET:
				return getScenarioSet();
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__NAME:
				setName((String)newValue);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE:
				setUsedModule((FunctionalityModule)newValue);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO:
				setScenario((Scenario)newValue);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET:
				setScenarioSet((ScenarioSet)newValue);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE:
				setUsedModule((FunctionalityModule)null);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO:
				setScenario((Scenario)null);
				return;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET:
				setScenarioSet((ScenarioSet)null);
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
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__USED_MODULE:
				return usedModule != null;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO:
				return scenario != null;
			case Functionality_modulePackage.FUNCTIONALITY_MODULE__SCENARIO_SET:
				return scenarioSet != null;
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //FunctionalityModuleImpl
