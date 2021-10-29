/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSet;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Service Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.ServiceTaskImpl#getRequiredAttributes <em>Required Attributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.ServiceTaskImpl#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.ServiceTaskImpl#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ServiceTaskImpl extends TaskImpl implements ServiceTask {
	/**
	 * The cached value of the '{@link #getRequiredAttributes() <em>Required Attributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequiredAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<CUBE_STRUCTURE_ITEM> requiredAttributes;

	/**
	 * The cached value of the '{@link #getEnrichedAttribute() <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEnrichedAttribute()
	 * @generated
	 * @ordered
	 */
	protected CUBE_STRUCTURE_ITEM enrichedAttribute;

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
	protected ServiceTaskImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.SERVICE_TASK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CUBE_STRUCTURE_ITEM> getRequiredAttributes() {
		if (requiredAttributes == null) {
			requiredAttributes = new EObjectResolvingEList<CUBE_STRUCTURE_ITEM>(CUBE_STRUCTURE_ITEM.class, this, Bpmn_litePackage.SERVICE_TASK__REQUIRED_ATTRIBUTES);
		}
		return requiredAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_STRUCTURE_ITEM getEnrichedAttribute() {
		if (enrichedAttribute != null && enrichedAttribute.eIsProxy()) {
			InternalEObject oldEnrichedAttribute = (InternalEObject)enrichedAttribute;
			enrichedAttribute = (CUBE_STRUCTURE_ITEM)eResolveProxy(oldEnrichedAttribute);
			if (enrichedAttribute != oldEnrichedAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE, oldEnrichedAttribute, enrichedAttribute));
			}
		}
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_STRUCTURE_ITEM basicGetEnrichedAttribute() {
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEnrichedAttribute(CUBE_STRUCTURE_ITEM newEnrichedAttribute) {
		CUBE_STRUCTURE_ITEM oldEnrichedAttribute = enrichedAttribute;
		enrichedAttribute = newEnrichedAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE, oldEnrichedAttribute, enrichedAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet getScenarioSet() {
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetScenarioSet(ScenarioSet newScenarioSet, NotificationChain msgs) {
		ScenarioSet oldScenarioSet = scenarioSet;
		scenarioSet = newScenarioSet;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET, oldScenarioSet, newScenarioSet);
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
	public void setScenarioSet(ScenarioSet newScenarioSet) {
		if (newScenarioSet != scenarioSet) {
			NotificationChain msgs = null;
			if (scenarioSet != null)
				msgs = ((InternalEObject)scenarioSet).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET, null, msgs);
			if (newScenarioSet != null)
				msgs = ((InternalEObject)newScenarioSet).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET, null, msgs);
			msgs = basicSetScenarioSet(newScenarioSet, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET, newScenarioSet, newScenarioSet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__REQUIRED_ATTRIBUTES:
				return getRequiredAttributes();
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				if (resolve) return getEnrichedAttribute();
				return basicGetEnrichedAttribute();
			case Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET:
				return getScenarioSet();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				getRequiredAttributes().addAll((Collection<? extends CUBE_STRUCTURE_ITEM>)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((CUBE_STRUCTURE_ITEM)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET:
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				return;
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((CUBE_STRUCTURE_ITEM)null);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET:
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__REQUIRED_ATTRIBUTES:
				return requiredAttributes != null && !requiredAttributes.isEmpty();
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				return enrichedAttribute != null;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIO_SET:
				return scenarioSet != null;
		}
		return super.eIsSet(featureID);
	}

} //ServiceTaskImpl
