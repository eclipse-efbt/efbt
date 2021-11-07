/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Service Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.ServiceTaskImpl#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.ServiceTaskImpl#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.ServiceTaskImpl#getLinkedAttributes <em>Linked Attributes</em>}</li>
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
	protected EAttribute enrichedAttribute;

	/**
	 * The cached value of the '{@link #getScenarios() <em>Scenarios</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarios()
	 * @generated
	 * @ordered
	 */
	protected EList<org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario> scenarios;

	/**
	 * The cached value of the '{@link #getLinkedAttributes() <em>Linked Attributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkedAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<EAttribute> linkedAttributes;

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
	public EAttribute getEnrichedAttribute() {
		if (enrichedAttribute != null && enrichedAttribute.eIsProxy()) {
			InternalEObject oldEnrichedAttribute = (InternalEObject)enrichedAttribute;
			enrichedAttribute = (EAttribute)eResolveProxy(oldEnrichedAttribute);
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
	public EAttribute basicGetEnrichedAttribute() {
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEnrichedAttribute(EAttribute newEnrichedAttribute) {
		EAttribute oldEnrichedAttribute = enrichedAttribute;
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
	public EList<org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario> getScenarios() {
		if (scenarios == null) {
			scenarios = new EObjectContainmentEList<org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario>(org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario.class, this, Bpmn_litePackage.SERVICE_TASK__SCENARIOS);
		}
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EAttribute> getLinkedAttributes() {
		if (linkedAttributes == null) {
			linkedAttributes = new EObjectResolvingEList<EAttribute>(EAttribute.class, this, Bpmn_litePackage.SERVICE_TASK__LINKED_ATTRIBUTES);
		}
		return linkedAttributes;
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
			case Bpmn_litePackage.SERVICE_TASK__LINKED_ATTRIBUTES:
				return getLinkedAttributes();
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
				setEnrichedAttribute((EAttribute)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				getScenarios().clear();
				getScenarios().addAll((Collection<? extends org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario>)newValue);
				return;
			case Bpmn_litePackage.SERVICE_TASK__LINKED_ATTRIBUTES:
				getLinkedAttributes().clear();
				getLinkedAttributes().addAll((Collection<? extends EAttribute>)newValue);
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
				setEnrichedAttribute((EAttribute)null);
				return;
			case Bpmn_litePackage.SERVICE_TASK__SCENARIOS:
				getScenarios().clear();
				return;
			case Bpmn_litePackage.SERVICE_TASK__LINKED_ATTRIBUTES:
				getLinkedAttributes().clear();
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
			case Bpmn_litePackage.SERVICE_TASK__LINKED_ATTRIBUTES:
				return linkedAttributes != null && !linkedAttributes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ServiceTaskImpl
