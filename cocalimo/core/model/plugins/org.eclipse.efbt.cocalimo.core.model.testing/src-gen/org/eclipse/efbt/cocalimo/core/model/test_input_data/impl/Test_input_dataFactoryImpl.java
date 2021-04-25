/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data.impl;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.*;

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
public class Test_input_dataFactoryImpl extends EFactoryImpl implements Test_input_dataFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Test_input_dataFactory init()
	{
		try {
			Test_input_dataFactory theTest_input_dataFactory = (Test_input_dataFactory)EPackage.Registry.INSTANCE.getEFactory(Test_input_dataPackage.eNS_URI);
			if (theTest_input_dataFactory != null) {
				return theTest_input_dataFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Test_input_dataFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test_input_dataFactoryImpl()
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
			case Test_input_dataPackage.INPUT_DATA_MODULE: return createInputDataModule();
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS: return createUsefulInputDataSnippets();
			case Test_input_dataPackage.TEST_INPUT_DATA: return createTestInputData();
			case Test_input_dataPackage.EXTERNAL_TEST_DATA: return createExternalTestData();
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE: return createUsefulInputDataSnippetsModule();
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
	public InputDataModule createInputDataModule()
	{
		InputDataModuleImpl inputDataModule = new InputDataModuleImpl();
		return inputDataModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public UsefulInputDataSnippets createUsefulInputDataSnippets()
	{
		UsefulInputDataSnippetsImpl usefulInputDataSnippets = new UsefulInputDataSnippetsImpl();
		return usefulInputDataSnippets;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestInputData createTestInputData()
	{
		TestInputDataImpl testInputData = new TestInputDataImpl();
		return testInputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ExternalTestData createExternalTestData()
	{
		ExternalTestDataImpl externalTestData = new ExternalTestDataImpl();
		return externalTestData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public UsefulInputDataSnippetsModule createUsefulInputDataSnippetsModule()
	{
		UsefulInputDataSnippetsModuleImpl usefulInputDataSnippetsModule = new UsefulInputDataSnippetsModuleImpl();
		return usefulInputDataSnippetsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test_input_dataPackage getTest_input_dataPackage()
	{
		return (Test_input_dataPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Test_input_dataPackage getPackage()
	{
		return Test_input_dataPackage.eINSTANCE;
	}

} //Test_input_dataFactoryImpl
