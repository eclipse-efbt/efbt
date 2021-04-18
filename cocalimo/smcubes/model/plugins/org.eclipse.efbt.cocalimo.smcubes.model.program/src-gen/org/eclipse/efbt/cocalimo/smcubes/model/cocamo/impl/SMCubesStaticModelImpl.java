/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesRegFunctionalityTestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getBddTestDefinitions <em>Bdd Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getTestTemplates <em>Test Templates</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getTestConstriants <em>Test Constriants</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getFunctionalityModules <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesStaticModelImpl extends StaticModelImpl implements SMCubesStaticModel
{
	/**
	 * The cached value of the '{@link #getSmCubesModel() <em>Sm Cubes Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmCubesModel()
	 * @generated
	 * @ordered
	 */
	protected SmcubesModel smCubesModel;

	/**
	 * The cached value of the '{@link #getRequirements() <em>Requirements</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequirements()
	 * @generated
	 * @ordered
	 */
	protected RequirementsModule requirements;

	/**
	 * The cached value of the '{@link #getBddTestDefinitions() <em>Bdd Test Definitions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBddTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected BDDTestDefinitionModule bddTestDefinitions;

	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected SMCubesBDDTestModule tests;

	/**
	 * The cached value of the '{@link #getTestTemplates() <em>Test Templates</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestTemplates()
	 * @generated
	 * @ordered
	 */
	protected BDDTestTemplateModule testTemplates;

	/**
	 * The cached value of the '{@link #getTestConstriants() <em>Test Constriants</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestConstriants()
	 * @generated
	 * @ordered
	 */
	protected BDDTestContraints testConstriants;

	/**
	 * The cached value of the '{@link #getFunctionalityModules() <em>Functionality Modules</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalityModules()
	 * @generated
	 * @ordered
	 */
	protected FunctionalityModuleModule functionalityModules;

	/**
	 * The cached value of the '{@link #getRegFunctionalityTests() <em>Reg Functionality Tests</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRegFunctionalityTests()
	 * @generated
	 * @ordered
	 */
	protected SMCubesRegFunctionalityTestModule regFunctionalityTests;

	/**
	 * The cached value of the '{@link #getRegFunctionalityTestDefinitions() <em>Reg Functionality Test Definitions</em>}' containment reference.
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
	protected SMCubesStaticModelImpl()
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
		return CocamoPackage.Literals.SM_CUBES_STATIC_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SmcubesModel getSmCubesModel()
	{
		return smCubesModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSmCubesModel(SmcubesModel newSmCubesModel, NotificationChain msgs)
	{
		SmcubesModel oldSmCubesModel = smCubesModel;
		smCubesModel = newSmCubesModel;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, oldSmCubesModel, newSmCubesModel);
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
	public void setSmCubesModel(SmcubesModel newSmCubesModel)
	{
		if (newSmCubesModel != smCubesModel) {
			NotificationChain msgs = null;
			if (smCubesModel != null)
				msgs = ((InternalEObject)smCubesModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, null, msgs);
			if (newSmCubesModel != null)
				msgs = ((InternalEObject)newSmCubesModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, null, msgs);
			msgs = basicSetSmCubesModel(newSmCubesModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL, newSmCubesModel, newSmCubesModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsModule getRequirements()
	{
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRequirements(RequirementsModule newRequirements, NotificationChain msgs)
	{
		RequirementsModule oldRequirements = requirements;
		requirements = newRequirements;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, oldRequirements, newRequirements);
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
	public void setRequirements(RequirementsModule newRequirements)
	{
		if (newRequirements != requirements) {
			NotificationChain msgs = null;
			if (requirements != null)
				msgs = ((InternalEObject)requirements).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, null, msgs);
			if (newRequirements != null)
				msgs = ((InternalEObject)newRequirements).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, null, msgs);
			msgs = basicSetRequirements(newRequirements, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS, newRequirements, newRequirements));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestDefinitionModule getBddTestDefinitions()
	{
		return bddTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBddTestDefinitions(BDDTestDefinitionModule newBddTestDefinitions, NotificationChain msgs)
	{
		BDDTestDefinitionModule oldBddTestDefinitions = bddTestDefinitions;
		bddTestDefinitions = newBddTestDefinitions;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, oldBddTestDefinitions, newBddTestDefinitions);
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
	public void setBddTestDefinitions(BDDTestDefinitionModule newBddTestDefinitions)
	{
		if (newBddTestDefinitions != bddTestDefinitions) {
			NotificationChain msgs = null;
			if (bddTestDefinitions != null)
				msgs = ((InternalEObject)bddTestDefinitions).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, null, msgs);
			if (newBddTestDefinitions != null)
				msgs = ((InternalEObject)newBddTestDefinitions).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, null, msgs);
			msgs = basicSetBddTestDefinitions(newBddTestDefinitions, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS, newBddTestDefinitions, newBddTestDefinitions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesBDDTestModule getTests()
	{
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetTests(SMCubesBDDTestModule newTests, NotificationChain msgs)
	{
		SMCubesBDDTestModule oldTests = tests;
		tests = newTests;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS, oldTests, newTests);
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
	public void setTests(SMCubesBDDTestModule newTests)
	{
		if (newTests != tests) {
			NotificationChain msgs = null;
			if (tests != null)
				msgs = ((InternalEObject)tests).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS, null, msgs);
			if (newTests != null)
				msgs = ((InternalEObject)newTests).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS, null, msgs);
			msgs = basicSetTests(newTests, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS, newTests, newTests));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestTemplateModule getTestTemplates()
	{
		return testTemplates;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetTestTemplates(BDDTestTemplateModule newTestTemplates, NotificationChain msgs)
	{
		BDDTestTemplateModule oldTestTemplates = testTemplates;
		testTemplates = newTestTemplates;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, oldTestTemplates, newTestTemplates);
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
	public void setTestTemplates(BDDTestTemplateModule newTestTemplates)
	{
		if (newTestTemplates != testTemplates) {
			NotificationChain msgs = null;
			if (testTemplates != null)
				msgs = ((InternalEObject)testTemplates).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, null, msgs);
			if (newTestTemplates != null)
				msgs = ((InternalEObject)newTestTemplates).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, null, msgs);
			msgs = basicSetTestTemplates(newTestTemplates, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES, newTestTemplates, newTestTemplates));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestContraints getTestConstriants()
	{
		return testConstriants;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetTestConstriants(BDDTestContraints newTestConstriants, NotificationChain msgs)
	{
		BDDTestContraints oldTestConstriants = testConstriants;
		testConstriants = newTestConstriants;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, oldTestConstriants, newTestConstriants);
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
	public void setTestConstriants(BDDTestContraints newTestConstriants)
	{
		if (newTestConstriants != testConstriants) {
			NotificationChain msgs = null;
			if (testConstriants != null)
				msgs = ((InternalEObject)testConstriants).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, null, msgs);
			if (newTestConstriants != null)
				msgs = ((InternalEObject)newTestConstriants).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, null, msgs);
			msgs = basicSetTestConstriants(newTestConstriants, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS, newTestConstriants, newTestConstriants));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModuleModule getFunctionalityModules()
	{
		return functionalityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFunctionalityModules(FunctionalityModuleModule newFunctionalityModules, NotificationChain msgs)
	{
		FunctionalityModuleModule oldFunctionalityModules = functionalityModules;
		functionalityModules = newFunctionalityModules;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES, oldFunctionalityModules, newFunctionalityModules);
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
	public void setFunctionalityModules(FunctionalityModuleModule newFunctionalityModules)
	{
		if (newFunctionalityModules != functionalityModules) {
			NotificationChain msgs = null;
			if (functionalityModules != null)
				msgs = ((InternalEObject)functionalityModules).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES, null, msgs);
			if (newFunctionalityModules != null)
				msgs = ((InternalEObject)newFunctionalityModules).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES, null, msgs);
			msgs = basicSetFunctionalityModules(newFunctionalityModules, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES, newFunctionalityModules, newFunctionalityModules));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesRegFunctionalityTestModule getRegFunctionalityTests()
	{
		return regFunctionalityTests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRegFunctionalityTests(SMCubesRegFunctionalityTestModule newRegFunctionalityTests, NotificationChain msgs)
	{
		SMCubesRegFunctionalityTestModule oldRegFunctionalityTests = regFunctionalityTests;
		regFunctionalityTests = newRegFunctionalityTests;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, oldRegFunctionalityTests, newRegFunctionalityTests);
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
	public void setRegFunctionalityTests(SMCubesRegFunctionalityTestModule newRegFunctionalityTests)
	{
		if (newRegFunctionalityTests != regFunctionalityTests) {
			NotificationChain msgs = null;
			if (regFunctionalityTests != null)
				msgs = ((InternalEObject)regFunctionalityTests).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, null, msgs);
			if (newRegFunctionalityTests != null)
				msgs = ((InternalEObject)newRegFunctionalityTests).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, null, msgs);
			msgs = basicSetRegFunctionalityTests(newRegFunctionalityTests, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS, newRegFunctionalityTests, newRegFunctionalityTests));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTestDefinitionModule getRegFunctionalityTestDefinitions()
	{
		return regFunctionalityTestDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule newRegFunctionalityTestDefinitions, NotificationChain msgs)
	{
		RegFunctionalityTestDefinitionModule oldRegFunctionalityTestDefinitions = regFunctionalityTestDefinitions;
		regFunctionalityTestDefinitions = newRegFunctionalityTestDefinitions;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, oldRegFunctionalityTestDefinitions, newRegFunctionalityTestDefinitions);
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
	public void setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule newRegFunctionalityTestDefinitions)
	{
		if (newRegFunctionalityTestDefinitions != regFunctionalityTestDefinitions) {
			NotificationChain msgs = null;
			if (regFunctionalityTestDefinitions != null)
				msgs = ((InternalEObject)regFunctionalityTestDefinitions).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, null, msgs);
			if (newRegFunctionalityTestDefinitions != null)
				msgs = ((InternalEObject)newRegFunctionalityTestDefinitions).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, null, msgs);
			msgs = basicSetRegFunctionalityTestDefinitions(newRegFunctionalityTestDefinitions, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS, newRegFunctionalityTestDefinitions, newRegFunctionalityTestDefinitions));
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
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				return basicSetSmCubesModel(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				return basicSetRequirements(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				return basicSetBddTestDefinitions(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
				return basicSetTests(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				return basicSetTestTemplates(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				return basicSetTestConstriants(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
				return basicSetFunctionalityModules(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				return basicSetRegFunctionalityTests(null, msgs);
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				return basicSetRegFunctionalityTestDefinitions(null, msgs);
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
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				return getSmCubesModel();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				return getRequirements();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				return getBddTestDefinitions();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
				return getTests();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				return getTestTemplates();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				return getTestConstriants();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
				return getFunctionalityModules();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				return getRegFunctionalityTests();
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				return getRegFunctionalityTestDefinitions();
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
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				setBddTestDefinitions((BDDTestDefinitionModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
				setTests((SMCubesBDDTestModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				setTestTemplates((BDDTestTemplateModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				setTestConstriants((BDDTestContraints)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
				setFunctionalityModules((FunctionalityModuleModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				setRegFunctionalityTests((SMCubesRegFunctionalityTestModule)newValue);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				setSmCubesModel((SmcubesModel)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				setRequirements((RequirementsModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				setBddTestDefinitions((BDDTestDefinitionModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
				setTests((SMCubesBDDTestModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				setTestTemplates((BDDTestTemplateModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				setTestConstriants((BDDTestContraints)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
				setFunctionalityModules((FunctionalityModuleModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				setRegFunctionalityTests((SMCubesRegFunctionalityTestModule)null);
				return;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
				return smCubesModel != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
				return requirements != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
				return bddTestDefinitions != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
				return tests != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
				return testTemplates != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
				return testConstriants != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
				return functionalityModules != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
				return regFunctionalityTests != null;
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
				return regFunctionalityTestDefinitions != null;
		}
		return super.eIsSet(featureID);
	}

} //SMCubesStaticModelImpl
