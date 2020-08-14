/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.functional_design.core.model.scenarios.Scenario;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.CodeCondition;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Scenario To Code Conditions</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.ScenarioToCodeConditionsImpl#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.ScenarioToCodeConditionsImpl#getCodeConditions <em>Code Conditions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScenarioToCodeConditionsImpl extends MinimalEObjectImpl.Container implements ScenarioToCodeConditions
{
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
	 * The cached value of the '{@link #getCodeConditions() <em>Code Conditions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCodeConditions()
	 * @generated
	 * @ordered
	 */
	protected EList<CodeCondition> codeConditions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScenarioToCodeConditionsImpl()
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
		return Domain_model_mappingPackage.Literals.SCENARIO_TO_CODE_CONDITIONS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario getScenario()
	{
		if (scenario != null && scenario.eIsProxy())
		{
			InternalEObject oldScenario = (InternalEObject)scenario;
			scenario = (Scenario)eResolveProxy(oldScenario);
			if (scenario != oldScenario)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO, oldScenario, scenario));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO, oldScenario, scenario));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CodeCondition> getCodeConditions()
	{
		if (codeConditions == null)
		{
			codeConditions = new EObjectContainmentEList<CodeCondition>(CodeCondition.class, this, Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS);
		}
		return codeConditions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS:
				return ((InternalEList<?>)getCodeConditions()).basicRemove(otherEnd, msgs);
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
		switch (featureID)
		{
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO:
				if (resolve) return getScenario();
				return basicGetScenario();
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS:
				return getCodeConditions();
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
		switch (featureID)
		{
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO:
				setScenario((Scenario)newValue);
				return;
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS:
				getCodeConditions().clear();
				getCodeConditions().addAll((Collection<? extends CodeCondition>)newValue);
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
		switch (featureID)
		{
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO:
				setScenario((Scenario)null);
				return;
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS:
				getCodeConditions().clear();
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
		switch (featureID)
		{
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__SCENARIO:
				return scenario != null;
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS__CODE_CONDITIONS:
				return codeConditions != null && !codeConditions.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ScenarioToCodeConditionsImpl
