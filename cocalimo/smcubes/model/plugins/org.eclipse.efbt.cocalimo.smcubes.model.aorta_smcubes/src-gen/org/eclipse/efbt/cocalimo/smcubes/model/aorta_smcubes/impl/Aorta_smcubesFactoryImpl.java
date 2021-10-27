/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.*;

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
public class Aorta_smcubesFactoryImpl extends EFactoryImpl implements Aorta_smcubesFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Aorta_smcubesFactory init() {
		try {
			Aorta_smcubesFactory theAorta_smcubesFactory = (Aorta_smcubesFactory)EPackage.Registry.INSTANCE.getEFactory(Aorta_smcubesPackage.eNS_URI);
			if (theAorta_smcubesFactory != null) {
				return theAorta_smcubesFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Aorta_smcubesFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Aorta_smcubesFactoryImpl() {
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
			case Aorta_smcubesPackage.AORTA_SM_CUBES_MODEL: return createAortaSMCubesModel();
			case Aorta_smcubesPackage.REPORT_RESULTS: return createReportResults();
			case Aorta_smcubesPackage.TEST: return createTest();
			case Aorta_smcubesPackage.TEST_MODULE: return createTestModule();
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE: return createTestDefinitionModule();
			case Aorta_smcubesPackage.TEST_DEFINITION: return createTestDefinition();
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
	public AortaSMCubesModel createAortaSMCubesModel() {
		AortaSMCubesModelImpl aortaSMCubesModel = new AortaSMCubesModelImpl();
		return aortaSMCubesModel;
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
	public TestDefinitionModule createTestDefinitionModule() {
		TestDefinitionModuleImpl testDefinitionModule = new TestDefinitionModuleImpl();
		return testDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestDefinition createTestDefinition() {
		TestDefinitionImpl testDefinition = new TestDefinitionImpl();
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Aorta_smcubesPackage getAorta_smcubesPackage() {
		return (Aorta_smcubesPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Aorta_smcubesPackage getPackage() {
		return Aorta_smcubesPackage.eINSTANCE;
	}

} //Aorta_smcubesFactoryImpl
