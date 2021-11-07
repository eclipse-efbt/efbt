/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.*;

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
public class Logical_transformationsFactoryImpl extends EFactoryImpl implements Logical_transformationsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Logical_transformationsFactory init() {
		try {
			Logical_transformationsFactory theLogical_transformationsFactory = (Logical_transformationsFactory)EPackage.Registry.INSTANCE.getEFactory(Logical_transformationsPackage.eNS_URI);
			if (theLogical_transformationsFactory != null) {
				return theLogical_transformationsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Logical_transformationsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Logical_transformationsFactoryImpl() {
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
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE: return createLogicalTransformationModule();
			case Logical_transformationsPackage.TASK_TAG: return createTaskTag();
			case Logical_transformationsPackage.SCENARIO: return createScenario();
			case Logical_transformationsPackage.SCENARIO_TAG: return createScenarioTag();
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
	public LogicalTransformationModule createLogicalTransformationModule() {
		LogicalTransformationModuleImpl logicalTransformationModule = new LogicalTransformationModuleImpl();
		return logicalTransformationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskTag createTaskTag() {
		TaskTagImpl taskTag = new TaskTagImpl();
		return taskTag;
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
	public Logical_transformationsPackage getLogical_transformationsPackage() {
		return (Logical_transformationsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Logical_transformationsPackage getPackage() {
		return Logical_transformationsPackage.eINSTANCE;
	}

} //Logical_transformationsFactoryImpl
