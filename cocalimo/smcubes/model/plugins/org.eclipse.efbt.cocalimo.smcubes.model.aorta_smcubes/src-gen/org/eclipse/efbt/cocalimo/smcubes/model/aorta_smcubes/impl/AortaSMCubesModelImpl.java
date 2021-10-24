/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.core.model.task.TaskModule;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Aorta SM Cubes Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getTaskModules <em>Task Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getTestDefinitions <em>Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AortaSMCubesModelImpl extends MinimalEObjectImpl.Container implements AortaSMCubesModel {
	/**
	 * The cached value of the '{@link #getSmCubesModel() <em>Sm Cubes Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmCubesModel()
	 * @generated
	 * @ordered
	 */
	protected SmcubesModel smCubesModel;

	/**
	 * The cached value of the '{@link #getRequirements() <em>Requirements</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequirements()
	 * @generated
	 * @ordered
	 */
	protected RequirementsModule requirements;

	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected TestModule tests;

	/**
	 * The cached value of the '{@link #getTaskModules() <em>Task Modules</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTaskModules()
	 * @generated
	 * @ordered
	 */
	protected TaskModule taskModules;

	/**
	 * The cached value of the '{@link #getTestDefinitions() <em>Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected TestDefinitionModule testDefinitions;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AortaSMCubesModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Aorta_smcubesPackage.Literals.AORTA_SM_CUBES_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SmcubesModel getSmCubesModel() {
		if (smCubesModel != null && smCubesModel.eIsProxy()) {
			InternalEObject oldSmCubesModel = (InternalEObject)smCubesModel;
			smCubesModel = (SmcubesModel)eResolveProxy(oldSmCubesModel);
			if (smCubesModel != oldSmCubesModel) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL, oldSmCubesModel, smCubesModel));
			}
		}
		return smCubesModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SmcubesModel basicGetSmCubesModel() {
		return smCubesModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSmCubesModel(SmcubesModel newSmCubesModel) {
		SmcubesModel oldSmCubesModel = smCubesModel;
		smCubesModel = newSmCubesModel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL, oldSmCubesModel, smCubesModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsModule getRequirements() {
		if (requirements != null && requirements.eIsProxy()) {
			InternalEObject oldRequirements = (InternalEObject)requirements;
			requirements = (RequirementsModule)eResolveProxy(oldRequirements);
			if (requirements != oldRequirements) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS, oldRequirements, requirements));
			}
		}
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsModule basicGetRequirements() {
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRequirements(RequirementsModule newRequirements) {
		RequirementsModule oldRequirements = requirements;
		requirements = newRequirements;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS, oldRequirements, requirements));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestModule getTests() {
		if (tests != null && tests.eIsProxy()) {
			InternalEObject oldTests = (InternalEObject)tests;
			tests = (TestModule)eResolveProxy(oldTests);
			if (tests != oldTests) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS, oldTests, tests));
			}
		}
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModule basicGetTests() {
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTests(TestModule newTests) {
		TestModule oldTests = tests;
		tests = newTests;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS, oldTests, tests));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskModule getTaskModules() {
		if (taskModules != null && taskModules.eIsProxy()) {
			InternalEObject oldTaskModules = (InternalEObject)taskModules;
			taskModules = (TaskModule)eResolveProxy(oldTaskModules);
			if (taskModules != oldTaskModules) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES, oldTaskModules, taskModules));
			}
		}
		return taskModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskModule basicGetTaskModules() {
		return taskModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTaskModules(TaskModule newTaskModules) {
		TaskModule oldTaskModules = taskModules;
		taskModules = newTaskModules;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES, oldTaskModules, taskModules));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestDefinitionModule getTestDefinitions() {
		if (testDefinitions != null && testDefinitions.eIsProxy()) {
			InternalEObject oldTestDefinitions = (InternalEObject)testDefinitions;
			testDefinitions = (TestDefinitionModule)eResolveProxy(oldTestDefinitions);
			if (testDefinitions != oldTestDefinitions) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS, oldTestDefinitions, testDefinitions));
			}
		}
		return testDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestDefinitionModule basicGetTestDefinitions() {
		return testDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestDefinitions(TestDefinitionModule newTestDefinitions) {
		TestDefinitionModule oldTestDefinitions = testDefinitions;
		testDefinitions = newTestDefinitions;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS, oldTestDefinitions, testDefinitions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL:
				if (resolve) return getSmCubesModel();
				return basicGetSmCubesModel();
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS:
				if (resolve) return getRequirements();
				return basicGetRequirements();
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS:
				if (resolve) return getTests();
				return basicGetTests();
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES:
				if (resolve) return getTaskModules();
				return basicGetTaskModules();
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS:
				if (resolve) return getTestDefinitions();
				return basicGetTestDefinitions();
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__NAME:
				return getName();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)newValue);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)newValue);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS:
				setTests((TestModule)newValue);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES:
				setTaskModules((TaskModule)newValue);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS:
				setTestDefinitions((TestDefinitionModule)newValue);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__NAME:
				setName((String)newValue);
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
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)null);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)null);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS:
				setTests((TestModule)null);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES:
				setTaskModules((TaskModule)null);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS:
				setTestDefinitions((TestDefinitionModule)null);
				return;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__NAME:
				setName(NAME_EDEFAULT);
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
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL:
				return smCubesModel != null;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__REQUIREMENTS:
				return requirements != null;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TESTS:
				return tests != null;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TASK_MODULES:
				return taskModules != null;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS:
				return testDefinitions != null;
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //AortaSMCubesModelImpl
