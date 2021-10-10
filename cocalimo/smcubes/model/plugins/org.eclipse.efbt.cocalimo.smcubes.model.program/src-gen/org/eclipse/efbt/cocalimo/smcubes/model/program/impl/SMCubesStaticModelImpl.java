/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.impl;

import org.eclipse.efbt.cocalimo.core.model.task.TaskModule;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes Static Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getBddTestDefinitions <em>Bdd Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getTestTemplates <em>Test Templates</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getTestConstriants <em>Test Constriants</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getTasks <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesStaticModelImpl extends StaticModelImpl implements SMCubesStaticModel {
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
	 * The cached value of the '{@link #getBddTestDefinitions() <em>Bdd Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBddTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected BDDTestDefinitionModule bddTestDefinitions;

	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected SMCubesBDDTestModule tests;

	/**
	 * The cached value of the '{@link #getTestTemplates() <em>Test Templates</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestTemplates()
	 * @generated
	 * @ordered
	 */
	protected BDDTestTemplateModule testTemplates;

	/**
	 * The cached value of the '{@link #getTestConstriants() <em>Test Constriants</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestConstriants()
	 * @generated
	 * @ordered
	 */
	protected BDDTestContraints testConstriants;

	/**
	 * The cached value of the '{@link #getTasks() <em>Functionality Modules</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTasks()
	 * @generated
	 * @ordered
	 */
	protected TaskModule functionalityModules;

	/**
	 * The cached value of the '{@link #getRegFunctionalityTests() <em>Reg Functionality Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRegFunctionalityTests()
	 * @generated
	 * @ordered
	 */
	protected SMCubesRegFunctionalityTestModule regFunctionalityTests;

	/**
	 * The cached value of the '{@link #getRegFunctionalityTestDefinitions() <em>Reg Functionality Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRegFunctionalityTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected RegFunctionalityTestDefinitionModule regFunctionalityTestDefinitions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesStaticModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ProgramPackage.Literals.SM_CUBES_STATIC_MODEL;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, oldSmCubesModel, smCubesModel));
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
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, oldSmCubesModel, smCubesModel));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, oldRequirements, requirements));
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
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, oldRequirements, requirements));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestDefinitionModule getBddTestDefinitions() {
		if (bddTestDefinitions != null && bddTestDefinitions.eIsProxy()) {
			InternalEObject oldBddTestDefinitions = (InternalEObject)bddTestDefinitions;
			bddTestDefinitions = (BDDTestDefinitionModule)eResolveProxy(oldBddTestDefinitions);
			if (bddTestDefinitions != oldBddTestDefinitions) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, oldBddTestDefinitions, bddTestDefinitions));
			}
		}
		return bddTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestDefinitionModule basicGetBddTestDefinitions() {
		return bddTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setBddTestDefinitions(BDDTestDefinitionModule newBddTestDefinitions) {
		BDDTestDefinitionModule oldBddTestDefinitions = bddTestDefinitions;
		bddTestDefinitions = newBddTestDefinitions;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, oldBddTestDefinitions, bddTestDefinitions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesBDDTestModule getTests() {
		if (tests != null && tests.eIsProxy()) {
			InternalEObject oldTests = (InternalEObject)tests;
			tests = (SMCubesBDDTestModule)eResolveProxy(oldTests);
			if (tests != oldTests) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS, oldTests, tests));
			}
		}
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesBDDTestModule basicGetTests() {
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTests(SMCubesBDDTestModule newTests) {
		SMCubesBDDTestModule oldTests = tests;
		tests = newTests;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS, oldTests, tests));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestTemplateModule getTestTemplates() {
		if (testTemplates != null && testTemplates.eIsProxy()) {
			InternalEObject oldTestTemplates = (InternalEObject)testTemplates;
			testTemplates = (BDDTestTemplateModule)eResolveProxy(oldTestTemplates);
			if (testTemplates != oldTestTemplates) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, oldTestTemplates, testTemplates));
			}
		}
		return testTemplates;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestTemplateModule basicGetTestTemplates() {
		return testTemplates;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestTemplates(BDDTestTemplateModule newTestTemplates) {
		BDDTestTemplateModule oldTestTemplates = testTemplates;
		testTemplates = newTestTemplates;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, oldTestTemplates, testTemplates));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestContraints getTestConstriants() {
		if (testConstriants != null && testConstriants.eIsProxy()) {
			InternalEObject oldTestConstriants = (InternalEObject)testConstriants;
			testConstriants = (BDDTestContraints)eResolveProxy(oldTestConstriants);
			if (testConstriants != oldTestConstriants) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, oldTestConstriants, testConstriants));
			}
		}
		return testConstriants;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestContraints basicGetTestConstriants() {
		return testConstriants;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestConstriants(BDDTestContraints newTestConstriants) {
		BDDTestContraints oldTestConstriants = testConstriants;
		testConstriants = newTestConstriants;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, oldTestConstriants, testConstriants));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskModule getTasks() {
		if (functionalityModules != null && functionalityModules.eIsProxy()) {
			InternalEObject oldTasks = (InternalEObject)functionalityModules;
			functionalityModules = (TaskModule)eResolveProxy(oldTasks);
			if (functionalityModules != oldTasks) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS, oldTasks, functionalityModules));
			}
		}
		return functionalityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskModule basicGetTasks() {
		return functionalityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTasks(TaskModule newTasks) {
		TaskModule oldTasks = functionalityModules;
		functionalityModules = newTasks;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS, oldTasks, functionalityModules));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesRegFunctionalityTestModule getRegFunctionalityTests() {
		if (regFunctionalityTests != null && regFunctionalityTests.eIsProxy()) {
			InternalEObject oldRegFunctionalityTests = (InternalEObject)regFunctionalityTests;
			regFunctionalityTests = (SMCubesRegFunctionalityTestModule)eResolveProxy(oldRegFunctionalityTests);
			if (regFunctionalityTests != oldRegFunctionalityTests) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, oldRegFunctionalityTests, regFunctionalityTests));
			}
		}
		return regFunctionalityTests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesRegFunctionalityTestModule basicGetRegFunctionalityTests() {
		return regFunctionalityTests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRegFunctionalityTests(SMCubesRegFunctionalityTestModule newRegFunctionalityTests) {
		SMCubesRegFunctionalityTestModule oldRegFunctionalityTests = regFunctionalityTests;
		regFunctionalityTests = newRegFunctionalityTests;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, oldRegFunctionalityTests, regFunctionalityTests));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTestDefinitionModule getRegFunctionalityTestDefinitions() {
		if (regFunctionalityTestDefinitions != null && regFunctionalityTestDefinitions.eIsProxy()) {
			InternalEObject oldRegFunctionalityTestDefinitions = (InternalEObject)regFunctionalityTestDefinitions;
			regFunctionalityTestDefinitions = (RegFunctionalityTestDefinitionModule)eResolveProxy(oldRegFunctionalityTestDefinitions);
			if (regFunctionalityTestDefinitions != oldRegFunctionalityTestDefinitions) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, oldRegFunctionalityTestDefinitions, regFunctionalityTestDefinitions));
			}
		}
		return regFunctionalityTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RegFunctionalityTestDefinitionModule basicGetRegFunctionalityTestDefinitions() {
		return regFunctionalityTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule newRegFunctionalityTestDefinitions) {
		RegFunctionalityTestDefinitionModule oldRegFunctionalityTestDefinitions = regFunctionalityTestDefinitions;
		regFunctionalityTestDefinitions = newRegFunctionalityTestDefinitions;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, oldRegFunctionalityTestDefinitions, regFunctionalityTestDefinitions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				if (resolve) return getSmCubesModel();
				return basicGetSmCubesModel();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				if (resolve) return getRequirements();
				return basicGetRequirements();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				if (resolve) return getBddTestDefinitions();
				return basicGetBddTestDefinitions();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS:
				if (resolve) return getTests();
				return basicGetTests();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				if (resolve) return getTestTemplates();
				return basicGetTestTemplates();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				if (resolve) return getTestConstriants();
				return basicGetTestConstriants();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS:
				if (resolve) return getTasks();
				return basicGetTasks();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				if (resolve) return getRegFunctionalityTests();
				return basicGetRegFunctionalityTests();
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				if (resolve) return getRegFunctionalityTestDefinitions();
				return basicGetRegFunctionalityTestDefinitions();
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
			case ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				setBddTestDefinitions((BDDTestDefinitionModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS:
				setTests((SMCubesBDDTestModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				setTestTemplates((BDDTestTemplateModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				setTestConstriants((BDDTestContraints)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS:
				setTasks((TaskModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				setRegFunctionalityTests((SMCubesRegFunctionalityTestModule)newValue);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				setRegFunctionalityTestDefinitions((RegFunctionalityTestDefinitionModule)newValue);
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
			case ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				setBddTestDefinitions((BDDTestDefinitionModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS:
				setTests((SMCubesBDDTestModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				setTestTemplates((BDDTestTemplateModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				setTestConstriants((BDDTestContraints)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS:
				setTasks((TaskModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				setRegFunctionalityTests((SMCubesRegFunctionalityTestModule)null);
				return;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				setRegFunctionalityTestDefinitions((RegFunctionalityTestDefinitionModule)null);
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
			case ProgramPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				return smCubesModel != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				return requirements != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				return bddTestDefinitions != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TESTS:
				return tests != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				return testTemplates != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				return testConstriants != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__TASKS:
				return functionalityModules != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				return regFunctionalityTests != null;
			case ProgramPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				return regFunctionalityTestDefinitions != null;
		}
		return super.eIsSet(featureID);
	}

} //SMCubesStaticModelImpl
