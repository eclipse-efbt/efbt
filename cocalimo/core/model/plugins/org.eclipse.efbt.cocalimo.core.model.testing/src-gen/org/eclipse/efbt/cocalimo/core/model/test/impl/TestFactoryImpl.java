/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.impl;

import org.eclipse.efbt.cocalimo.core.model.test.*;

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
public class TestFactoryImpl extends EFactoryImpl implements TestFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TestFactory init()
	{
		try {
			TestFactory theTestFactory = (TestFactory)EPackage.Registry.INSTANCE.getEFactory(TestPackage.eNS_URI);
			if (theTestFactory != null) {
				return theTestFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestFactoryImpl()
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
			case TestPackage.RESULT: return createResult();
			case TestPackage.BDD_TEST: return createBDDTest();
			case TestPackage.REG_FUNCTIONALITY_TEST: return createRegFunctionalityTest();
			case TestPackage.TEST_MODULE: return createTestModule();
			case TestPackage.TEST: return createTest();
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
	public Result createResult()
	{
		ResultImpl result = new ResultImpl();
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTest createBDDTest()
	{
		BDDTestImpl bddTest = new BDDTestImpl();
		return bddTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTest createRegFunctionalityTest()
	{
		RegFunctionalityTestImpl regFunctionalityTest = new RegFunctionalityTestImpl();
		return regFunctionalityTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestModule createTestModule()
	{
		TestModuleImpl testModule = new TestModuleImpl();
		return testModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test createTest()
	{
		TestImpl test = new TestImpl();
		return test;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestPackage getTestPackage()
	{
		return (TestPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TestPackage getPackage()
	{
		return TestPackage.eINSTANCE;
	}

} //TestFactoryImpl
