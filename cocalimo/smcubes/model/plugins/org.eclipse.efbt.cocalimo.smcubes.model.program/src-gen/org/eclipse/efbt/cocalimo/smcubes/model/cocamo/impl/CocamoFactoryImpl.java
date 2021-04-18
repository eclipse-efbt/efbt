/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.*;

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
public class CocamoFactoryImpl extends EFactoryImpl implements CocamoFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static CocamoFactory init()
	{
		try {
			CocamoFactory theCocamoFactory = (CocamoFactory)EPackage.Registry.INSTANCE.getEFactory(CocamoPackage.eNS_URI);
			if (theCocamoFactory != null) {
				return theCocamoFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new CocamoFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CocamoFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case CocamoPackage.SM_CUBES_STATIC_MODEL: return createSMCubesStaticModel();
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA: return createSMCubesTestInputData();
			case CocamoPackage.SM_CUBES_REPORT_RESULTS: return createSMCubesReportResults();
			case CocamoPackage.SM_CUBES_BDD_TEST: return createSMCubesBDDTest();
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE: return createSMCubesBDDTestModule();
			case CocamoPackage.STATIC_MODEL: return createStaticModel();
			case CocamoPackage.SM_CUBES_REG_FUNCTIONALITY_TEST: return createSMCubesRegFunctionalityTest();
			case CocamoPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE: return createSMCubesRegFunctionalityTestModule();
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
	public SMCubesStaticModel createSMCubesStaticModel()
	{
		SMCubesStaticModelImpl smCubesStaticModel = new SMCubesStaticModelImpl();
		return smCubesStaticModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesTestInputData createSMCubesTestInputData()
	{
		SMCubesTestInputDataImpl smCubesTestInputData = new SMCubesTestInputDataImpl();
		return smCubesTestInputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesReportResults createSMCubesReportResults()
	{
		SMCubesReportResultsImpl smCubesReportResults = new SMCubesReportResultsImpl();
		return smCubesReportResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesBDDTest createSMCubesBDDTest()
	{
		SMCubesBDDTestImpl smCubesBDDTest = new SMCubesBDDTestImpl();
		return smCubesBDDTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesBDDTestModule createSMCubesBDDTestModule()
	{
		SMCubesBDDTestModuleImpl smCubesBDDTestModule = new SMCubesBDDTestModuleImpl();
		return smCubesBDDTestModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StaticModel createStaticModel()
	{
		StaticModelImpl staticModel = new StaticModelImpl();
		return staticModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesRegFunctionalityTest createSMCubesRegFunctionalityTest()
	{
		SMCubesRegFunctionalityTestImpl smCubesRegFunctionalityTest = new SMCubesRegFunctionalityTestImpl();
		return smCubesRegFunctionalityTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesRegFunctionalityTestModule createSMCubesRegFunctionalityTestModule()
	{
		SMCubesRegFunctionalityTestModuleImpl smCubesRegFunctionalityTestModule = new SMCubesRegFunctionalityTestModuleImpl();
		return smCubesRegFunctionalityTestModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CocamoPackage getCocamoPackage()
	{
		return (CocamoPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static CocamoPackage getPackage()
	{
		return CocamoPackage.eINSTANCE;
	}

} //CocamoFactoryImpl
