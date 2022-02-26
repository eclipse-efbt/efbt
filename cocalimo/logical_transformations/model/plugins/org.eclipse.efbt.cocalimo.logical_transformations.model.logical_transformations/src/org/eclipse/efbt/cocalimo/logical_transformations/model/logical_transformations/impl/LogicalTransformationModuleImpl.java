/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.ActivityTag;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.ScenarioTag;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.TestModule;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.LogicalTransformationModuleImpl#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.LogicalTransformationModuleImpl#getScenarioTags <em>Scenario Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.LogicalTransformationModuleImpl#getSubProcess <em>Sub Process</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.LogicalTransformationModuleImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.LogicalTransformationModuleImpl#getTestModules <em>Test Modules</em>}</li>
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
		return Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ActivityTag> getTaskTags() {
		if (taskTags == null) {
			taskTags = new EObjectContainmentEList<ActivityTag>(ActivityTag.class, this, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS);
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
			scenarioTags = new EObjectContainmentEList<ScenarioTag>(ScenarioTag.class, this, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS);
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
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, oldSubProcess, newSubProcess);
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
				msgs = ((InternalEObject)subProcess).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, null, msgs);
			if (newSubProcess != null)
				msgs = ((InternalEObject)newSubProcess).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, null, msgs);
			msgs = basicSetSubProcess(newSubProcess, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS, newSubProcess, newSubProcess));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementsModule> getRequirements() {
		if (requirements == null) {
			requirements = new EObjectContainmentEList<RequirementsModule>(RequirementsModule.class, this, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS);
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
			testModules = new EObjectResolvingEList<TestModule>(TestModule.class, this, Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES);
		}
		return testModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return ((InternalEList<?>)getTaskTags()).basicRemove(otherEnd, msgs);
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return ((InternalEList<?>)getScenarioTags()).basicRemove(otherEnd, msgs);
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return basicSetSubProcess(null, msgs);
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return ((InternalEList<?>)getRequirements()).basicRemove(otherEnd, msgs);
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
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return getTaskTags();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return getScenarioTags();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return getSubProcess();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return getRequirements();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				return getTestModules();
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
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				getTaskTags().clear();
				getTaskTags().addAll((Collection<? extends ActivityTag>)newValue);
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				getScenarioTags().clear();
				getScenarioTags().addAll((Collection<? extends ScenarioTag>)newValue);
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				setSubProcess((SubProcess)newValue);
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				getRequirements().clear();
				getRequirements().addAll((Collection<? extends RequirementsModule>)newValue);
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				getTestModules().clear();
				getTestModules().addAll((Collection<? extends TestModule>)newValue);
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
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				getTaskTags().clear();
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				getScenarioTags().clear();
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				setSubProcess((SubProcess)null);
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				getRequirements().clear();
				return;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				getTestModules().clear();
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
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
				return taskTags != null && !taskTags.isEmpty();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
				return scenarioTags != null && !scenarioTags.isEmpty();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS:
				return subProcess != null;
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
				return requirements != null && !requirements.isEmpty();
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES:
				return testModules != null && !testModules.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //LogicalTransformationModuleImpl
