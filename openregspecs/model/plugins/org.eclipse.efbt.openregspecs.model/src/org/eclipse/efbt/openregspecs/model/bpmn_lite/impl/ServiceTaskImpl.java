/**
 */
package org.eclipse.efbt.openregspecs.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute;

import org.eclipse.efbt.openregspecs.model.testing.Scenario;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Service Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.ServiceTaskImpl#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.ServiceTaskImpl#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.ServiceTaskImpl#getSecondAttribute <em>Second Attribute</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ServiceTaskImpl extends TaskImpl implements ServiceTask {
	/**
	 * The cached value of the '{@link #getEnrichedAttribute() <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEnrichedAttribute()
	 * @generated
	 * @ordered
	 */
	protected Attribute enrichedAttribute;

	/**
	 * The cached value of the '{@link #getScenarios() <em>Scenarios</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarios()
	 * @generated
	 * @ordered
	 */
	protected EList<Scenario> scenarios;

	/**
	 * The cached value of the '{@link #getSecondAttribute() <em>Second Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSecondAttribute()
	 * @generated
	 * @ordered
	 */
	protected Attribute secondAttribute;

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
	public Attribute getEnrichedAttribute() {
		if (enrichedAttribute != null && enrichedAttribute.eIsProxy()) {
			InternalEObject oldEnrichedAttribute = (InternalEObject)enrichedAttribute;
			enrichedAttribute = (Attribute)eResolveProxy(oldEnrichedAttribute);
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
	public Attribute basicGetEnrichedAttribute() {
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEnrichedAttribute(Attribute newEnrichedAttribute) {
		Attribute oldEnrichedAttribute = enrichedAttribute;
		enrichedAttribute = newEnrichedAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE, oldEnrichedAttribute, enrichedAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Scenario> getScenarios() {
		if (scenarios == null) {
			scenarios = new EObjectContainmentEList<Scenario>(Scenario.class, this, Bpmn_litePackage.SERVICE_TASK__SCENARIOS);
		}
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute getSecondAttribute() {
		if (secondAttribute != null && secondAttribute.eIsProxy()) {
			InternalEObject oldSecondAttribute = (InternalEObject)secondAttribute;
			secondAttribute = (Attribute)eResolveProxy(oldSecondAttribute);
			if (secondAttribute != oldSecondAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE, oldSecondAttribute, secondAttribute));
			}
		}
		return secondAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute basicGetSecondAttribute() {
		return secondAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSecondAttribute(Attribute newSecondAttribute) {
		Attribute oldSecondAttribute = secondAttribute;
		secondAttribute = newSecondAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE, oldSecondAttribute, secondAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				return ((InternalEList<?>)getScenarios()).basicRemove(otherEnd, msgs);
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
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				if (resolve) return getEnrichedAttribute();
				return basicGetEnrichedAttribute();
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				return getScenarios();
			case Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE:
				if (resolve) return getSecondAttribute();
				return basicGetSecondAttribute();
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
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((Attribute)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				getScenarios().clear();
				getScenarios().addAll((Collection<? extends Scenario>)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE:
				setSecondAttribute((Attribute)newValue);
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
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((Attribute)null);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				getScenarios().clear();
				return;
			case Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE:
				setSecondAttribute((Attribute)null);
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
			case Bpmn_litePackage.SERVICE_TASK__ENRICHED_ATTRIBUTE:
				return enrichedAttribute != null;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				return scenarios != null && !scenarios.isEmpty();
			case Bpmn_litePackage.SERVICE_TASK__SECOND_ATTRIBUTE:
				return secondAttribute != null;
		}
		return super.eIsSet(featureID);
	}

} //ServiceTaskImpl
