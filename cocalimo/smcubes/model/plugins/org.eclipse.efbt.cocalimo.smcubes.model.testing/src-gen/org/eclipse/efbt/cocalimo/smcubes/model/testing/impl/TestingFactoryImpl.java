/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.testing.*;

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
			case TestingPackage.CELL_WITH_ENUMERATED_VALUE: return createCellWithEnumeratedValue();
			case TestingPackage.CELL_WITH_VALUE: return createCellWithValue();
			case TestingPackage.INPUT_DATA: return createInputData();
			case TestingPackage.CELL: return createCell();
			case TestingPackage.CUBE_DATA: return createCubeData();
			case TestingPackage.ROW_DATA: return createRowData();
			case TestingPackage.TEST_MODULE: return createTestModule();
			case TestingPackage.REPORT_RESULTS: return createReportResults();
			case TestingPackage.TEST: return createTest();
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
	public CellWithEnumeratedValue createCellWithEnumeratedValue() {
		CellWithEnumeratedValueImpl cellWithEnumeratedValue = new CellWithEnumeratedValueImpl();
		return cellWithEnumeratedValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CellWithValue createCellWithValue() {
		CellWithValueImpl cellWithValue = new CellWithValueImpl();
		return cellWithValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputData createInputData() {
		InputDataImpl inputData = new InputDataImpl();
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cell createCell() {
		CellImpl cell = new CellImpl();
		return cell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeData createCubeData() {
		CubeDataImpl cubeData = new CubeDataImpl();
		return cubeData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RowData createRowData() {
		RowDataImpl rowData = new RowDataImpl();
		return rowData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestModule createTestModule() {
		TestModuleImpl testModule = new TestModuleImpl();
		return testModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportResults createReportResults() {
		ReportResultsImpl reportResults = new ReportResultsImpl();
		return reportResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test createTest() {
		TestImpl test = new TestImpl();
		return test;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
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
