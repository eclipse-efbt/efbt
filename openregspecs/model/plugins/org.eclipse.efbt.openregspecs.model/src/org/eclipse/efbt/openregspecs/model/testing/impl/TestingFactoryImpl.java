/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import org.eclipse.efbt.openregspecs.model.testing.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
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
public class TestingFactoryImpl extends EFactoryImpl implements TestingFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TestingFactory init() {
		try {
			TestingFactory theTestingFactory = (TestingFactory)EPackage.Registry.INSTANCE.getEFactory(TestingPackage.eNS_URI);
			if (theTestingFactory != null) {
				return theTestingFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestingFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestingFactoryImpl() {
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
			case TestingPackage.LOGICAL_TRANSFORMATION_MODULE: return createLogicalTransformationModule();
			case TestingPackage.ACTIVITY_TAG: return createActivityTag();
			case TestingPackage.SCENARIO: return createScenario();
			case TestingPackage.SCENARIO_TAG: return createScenarioTag();
			case TestingPackage.TEST: return createTest();
			case TestingPackage.TEST_MODULE: return createTestModule();
			case TestingPackage.DATA_CONSTRAINT: return createDataConstraint();
			case TestingPackage.SELECTION_LAYER: return createSelectionLayer();
			case TestingPackage.TEST_SCOPE: return createTestScope();
			case TestingPackage.UNIT_TEST_SCOPE: return createUnitTestScope();
			case TestingPackage.E2E_TEST_SCOPE: return createE2ETestScope();
			case TestingPackage.CSV_FILE: return createCSVFile();
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
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case TestingPackage.ATTR_COMPARISON:
				return createAttrComparisonFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case TestingPackage.ATTR_COMPARISON:
				return convertAttrComparisonToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LogicalTransformationModule createLogicalTransformationModule() {
		LogicalTransformationModuleImpl logicalTransformationModule = new LogicalTransformationModuleImpl();
		return logicalTransformationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ActivityTag createActivityTag() {
		ActivityTagImpl activityTag = new ActivityTagImpl();
		return activityTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario createScenario() {
		ScenarioImpl scenario = new ScenarioImpl();
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ScenarioTag createScenarioTag() {
		ScenarioTagImpl scenarioTag = new ScenarioTagImpl();
		return scenarioTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test createTest() {
		TestImpl test = new TestImpl();
		return test;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModule createTestModule() {
		TestModuleImpl testModule = new TestModuleImpl();
		return testModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DataConstraint createDataConstraint() {
		DataConstraintImpl dataConstraint = new DataConstraintImpl();
		return dataConstraint;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectionLayer createSelectionLayer() {
		SelectionLayerImpl selectionLayer = new SelectionLayerImpl();
		return selectionLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestScope createTestScope() {
		TestScopeImpl testScope = new TestScopeImpl();
		return testScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnitTestScope createUnitTestScope() {
		UnitTestScopeImpl unitTestScope = new UnitTestScopeImpl();
		return unitTestScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public E2ETestScope createE2ETestScope() {
		E2ETestScopeImpl e2ETestScope = new E2ETestScopeImpl();
		return e2ETestScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CSVFile createCSVFile() {
		CSVFileImpl csvFile = new CSVFileImpl();
		return csvFile;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttrComparison createAttrComparisonFromString(EDataType eDataType, String initialValue) {
		AttrComparison result = AttrComparison.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertAttrComparisonToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestingPackage getTestingPackage() {
		return (TestingPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TestingPackage getPackage() {
		return TestingPackage.eINSTANCE;
	}

} //TestingFactoryImpl
