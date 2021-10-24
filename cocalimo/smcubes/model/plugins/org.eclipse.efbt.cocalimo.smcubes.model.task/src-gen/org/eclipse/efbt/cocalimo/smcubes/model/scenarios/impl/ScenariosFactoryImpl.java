/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.scenarios.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class ScenariosFactoryImpl extends EFactoryImpl implements ScenariosFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static ScenariosFactory init() {
		try {
			ScenariosFactory theScenariosFactory = (ScenariosFactory)EPackage.Registry.INSTANCE.getEFactory(ScenariosPackage.eNS_URI);
			if (theScenariosFactory != null) {
				return theScenariosFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new ScenariosFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ScenariosFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case ScenariosPackage.SCENARIO: return createScenario();
			case ScenariosPackage.SCENARIO_SET: return createScenarioSet();
			case ScenariosPackage.SCENARIO_TAG: return createScenarioTag();
			case ScenariosPackage.SCENARIO_SET_TAG: return createScenarioSetTag();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario createScenario() {
		ScenarioImpl scenario = new ScenarioImpl();
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet createScenarioSet() {
		ScenarioSetImpl scenarioSet = new ScenarioSetImpl();
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioTag createScenarioTag() {
		ScenarioTagImpl scenarioTag = new ScenarioTagImpl();
		return scenarioTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSetTag createScenarioSetTag() {
		ScenarioSetTagImpl scenarioSetTag = new ScenarioSetTagImpl();
		return scenarioSetTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenariosPackage getScenariosPackage() {
		return (ScenariosPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static ScenariosPackage getPackage() {
		return ScenariosPackage.eINSTANCE;
	}

} //ScenariosFactoryImpl
