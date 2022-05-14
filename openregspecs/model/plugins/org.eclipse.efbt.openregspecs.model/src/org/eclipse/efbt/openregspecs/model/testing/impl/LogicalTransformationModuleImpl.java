/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.openregspecs.model.testing.ActivityTag;
import org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule;
import org.eclipse.efbt.openregspecs.model.testing.ScenarioTag;
import org.eclipse.efbt.openregspecs.model.testing.SelectionLayer;
import org.eclipse.efbt.openregspecs.model.testing.TestModule;
import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Logical Transformation Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getScenarioTags <em>Scenario Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getSubProcess <em>Sub Process</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getTestModules <em>Test Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl#getSelectionLayers <em>Selection Layers</em>}</li>
 * </ul>
 *
 * @generated
 */
public class LogicalTransformationModuleImpl extends ModuleImpl implements LogicalTransformationModule {
	/**
	 * The cached value of the '{@link #getTaskTags() <em>Task Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTaskTags()
	 * @generated
	 * @ordered
	 */
	protected EList<ActivityTag> taskTags;

	/**
	 * The cached value of the '{@link #getScenarioTags() <em>Scenario Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioTags()
	 * @generated
	 * @ordered
	 */
	protected EList<ScenarioTag> scenarioTags;

	/**
	 * The cached value of the '{@link #getSubProcess() <em>Sub Process</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubProcess()
	 * @generated
	 * @ordered
	 */
	protected SubProcess subProcess;

	/**
	 * The cached value of the '{@link #getRequirements() <em>Requirements</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequirements()
	 * @generated
	 * @ordered
	 */
	protected EList<RequirementsModule> requirements;

	/**
	 * The cached value of the '{@link #getTestModules() <em>Test Modules</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestModules()
	 * @generated
	 * @ordered
	 */
	protected EList<TestModule> testModules;

	/**
	 * The cached value of the '{@link #getSelectionLayers() <em>Selection Layers</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSelectionLayers()
	 * @generated
	 * @ordered
	 */
	protected EList<SelectionLayer> selectionLayers;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected LogicalTransformationModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.LOGICAL_TRANSFORMATION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ActivityTag> getTaskTags() {
		if (taskTags == null) {
			taskTags = new EObjectContainmentEList<ActivityTag>(ActivityTag.class, this, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS);
		}
		return taskTags;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ScenarioTag> getScenarioTags() {
		if (scenarioTags == null) {
			scenarioTags = new EObjectContainmentEList<ScenarioTag>(ScenarioTag.class, this, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS);
		}
		return scenarioTags;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SubProcess getSubProcess() {
		return subProcess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSubProcess(SubProcess newSubProcess, NotificationChain msgs) {
		SubProcess oldSubProcess = subProcess;
		subProcess = newSubProcess;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, oldSubProcess, newSubProcess);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSubProcess(SubProcess newSubProcess) {
		if (newSubProcess != subProcess) {
			NotificationChain msgs = null;
			if (subProcess != null)
				msgs = ((InternalEObject)subProcess).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, null, msgs);
			if (newSubProcess != null)
				msgs = ((InternalEObject)newSubProcess).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, null, msgs);
			msgs = basicSetSubProcess(newSubProcess, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, newSubProcess, newSubProcess));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementsModule> getRequirements() {
		if (requirements == null) {
			requirements = new EObjectContainmentEList<RequirementsModule>(RequirementsModule.class, this, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS);
		}
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TestModule> getTestModules() {
		if (testModules == null) {
			testModules = new EObjectResolvingEList<TestModule>(TestModule.class, this, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES);
		}
		return testModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SelectionLayer> getSelectionLayers() {
		if (selectionLayers == null) {
			selectionLayers = new EObjectContainmentEList<SelectionLayer>(SelectionLayer.class, this, TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS);
		}
		return selectionLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return ((InternalEList<?>)getTaskTags()).basicRemove(otherEnd, msgs);
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return ((InternalEList<?>)getScenarioTags()).basicRemove(otherEnd, msgs);
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return basicSetSubProcess(null, msgs);
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return ((InternalEList<?>)getRequirements()).basicRemove(otherEnd, msgs);
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS:
				return ((InternalEList<?>)getSelectionLayers()).basicRemove(otherEnd, msgs);
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
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return getTaskTags();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return getScenarioTags();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return getSubProcess();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return getRequirements();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				return getTestModules();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS:
				return getSelectionLayers();
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
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				getTaskTags().clear();
				getTaskTags().addAll((Collection<? extends ActivityTag>)newValue);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				getScenarioTags().clear();
				getScenarioTags().addAll((Collection<? extends ScenarioTag>)newValue);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				setSubProcess((SubProcess)newValue);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				getRequirements().clear();
				getRequirements().addAll((Collection<? extends RequirementsModule>)newValue);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				getTestModules().clear();
				getTestModules().addAll((Collection<? extends TestModule>)newValue);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS:
				getSelectionLayers().clear();
				getSelectionLayers().addAll((Collection<? extends SelectionLayer>)newValue);
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
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				getTaskTags().clear();
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				getScenarioTags().clear();
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				setSubProcess((SubProcess)null);
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				getRequirements().clear();
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				getTestModules().clear();
				return;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS:
				getSelectionLayers().clear();
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
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return taskTags != null && !taskTags.isEmpty();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return scenarioTags != null && !scenarioTags.isEmpty();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return subProcess != null;
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return requirements != null && !requirements.isEmpty();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				return testModules != null && !testModules.isEmpty();
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS:
				return selectionLayers != null && !selectionLayers.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //LogicalTransformationModuleImpl
