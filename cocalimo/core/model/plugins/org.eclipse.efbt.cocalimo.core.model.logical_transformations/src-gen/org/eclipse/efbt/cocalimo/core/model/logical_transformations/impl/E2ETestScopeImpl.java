/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.E2ETestScope;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.SelectionLayer;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>E2E Test Scope</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.E2ETestScopeImpl#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.E2ETestScopeImpl#getLayer <em>Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.E2ETestScopeImpl#getScriptTask <em>Script Task</em>}</li>
 * </ul>
 *
 * @generated
 */
public class E2ETestScopeImpl extends TestScopeImpl implements E2ETestScope {
	/**
	 * The cached value of the '{@link #getScenarios() <em>Scenarios</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarios()
	 * @generated
	 * @ordered
	 */
	protected EList<Scenario> scenarios;

	/**
	 * The cached value of the '{@link #getLayer() <em>Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLayer()
	 * @generated
	 * @ordered
	 */
	protected SelectionLayer layer;

	/**
	 * The cached value of the '{@link #getScriptTask() <em>Script Task</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScriptTask()
	 * @generated
	 * @ordered
	 */
	protected ScriptTask scriptTask;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected E2ETestScopeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Logical_transformationsPackage.Literals.E2E_TEST_SCOPE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Scenario> getScenarios() {
		if (scenarios == null) {
			scenarios = new EObjectResolvingEList<Scenario>(Scenario.class, this, Logical_transformationsPackage.E2E_TEST_SCOPE__SCENARIOS);
		}
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectionLayer getLayer() {
		if (layer != null && layer.eIsProxy()) {
			InternalEObject oldLayer = (InternalEObject)layer;
			layer = (SelectionLayer)eResolveProxy(oldLayer);
			if (layer != oldLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER, oldLayer, layer));
			}
		}
		return layer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectionLayer basicGetLayer() {
		return layer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLayer(SelectionLayer newLayer) {
		SelectionLayer oldLayer = layer;
		layer = newLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER, oldLayer, layer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScriptTask getScriptTask() {
		if (scriptTask != null && scriptTask.eIsProxy()) {
			InternalEObject oldScriptTask = (InternalEObject)scriptTask;
			scriptTask = (ScriptTask)eResolveProxy(oldScriptTask);
			if (scriptTask != oldScriptTask) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK, oldScriptTask, scriptTask));
			}
		}
		return scriptTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ScriptTask basicGetScriptTask() {
		return scriptTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScriptTask(ScriptTask newScriptTask) {
		ScriptTask oldScriptTask = scriptTask;
		scriptTask = newScriptTask;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK, oldScriptTask, scriptTask));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCENARIOS:
				return getScenarios();
			case Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER:
				if (resolve) return getLayer();
				return basicGetLayer();
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK:
				if (resolve) return getScriptTask();
				return basicGetScriptTask();
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
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCENARIOS:
				getScenarios().clear();
				getScenarios().addAll((Collection<? extends Scenario>)newValue);
				return;
			case Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER:
				setLayer((SelectionLayer)newValue);
				return;
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK:
				setScriptTask((ScriptTask)newValue);
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
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCENARIOS:
				getScenarios().clear();
				return;
			case Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER:
				setLayer((SelectionLayer)null);
				return;
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK:
				setScriptTask((ScriptTask)null);
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
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCENARIOS:
				return scenarios != null && !scenarios.isEmpty();
			case Logical_transformationsPackage.E2E_TEST_SCOPE__LAYER:
				return layer != null;
			case Logical_transformationsPackage.E2E_TEST_SCOPE__SCRIPT_TASK:
				return scriptTask != null;
		}
		return super.eIsSet(featureID);
	}

} //E2ETestScopeImpl
