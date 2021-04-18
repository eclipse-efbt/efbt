/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.util;

import org.eclipse.efbt.cocalimo.core.model.test.BDDTest;
import org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest;
import org.eclipse.efbt.cocalimo.core.model.test.Result;
import org.eclipse.efbt.cocalimo.core.model.test.Test;
import org.eclipse.efbt.cocalimo.core.model.test.TestModule;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage
 * @generated
 */
public class CocamoSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static CocamoPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CocamoSwitch()
	{
		if (modelPackage == null) {
			modelPackage = CocamoPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage)
	{
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject)
	{
		switch (classifierID) {
			case CocamoPackage.SM_CUBES_STATIC_MODEL: {
				SMCubesStaticModel smCubesStaticModel = (SMCubesStaticModel)theEObject;
				T result = caseSMCubesStaticModel(smCubesStaticModel);
				if (result == null) result = caseStaticModel(smCubesStaticModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA: {
				SMCubesTestInputData smCubesTestInputData = (SMCubesTestInputData)theEObject;
				T result = caseSMCubesTestInputData(smCubesTestInputData);
				if (result == null) result = caseTestInputData(smCubesTestInputData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_REPORT_RESULTS: {
				SMCubesReportResults smCubesReportResults = (SMCubesReportResults)theEObject;
				T result = caseSMCubesReportResults(smCubesReportResults);
				if (result == null) result = caseResult(smCubesReportResults);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_BDD_TEST: {
				SMCubesBDDTest smCubesBDDTest = (SMCubesBDDTest)theEObject;
				T result = caseSMCubesBDDTest(smCubesBDDTest);
				if (result == null) result = caseBDDTest(smCubesBDDTest);
				if (result == null) result = caseTest(smCubesBDDTest);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE: {
				SMCubesBDDTestModule smCubesBDDTestModule = (SMCubesBDDTestModule)theEObject;
				T result = caseSMCubesBDDTestModule(smCubesBDDTestModule);
				if (result == null) result = caseTestModule(smCubesBDDTestModule);
				if (result == null) result = caseModule(smCubesBDDTestModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.STATIC_MODEL: {
				StaticModel staticModel = (StaticModel)theEObject;
				T result = caseStaticModel(staticModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_REG_FUNCTIONALITY_TEST: {
				SMCubesRegFunctionalityTest smCubesRegFunctionalityTest = (SMCubesRegFunctionalityTest)theEObject;
				T result = caseSMCubesRegFunctionalityTest(smCubesRegFunctionalityTest);
				if (result == null) result = caseRegFunctionalityTest(smCubesRegFunctionalityTest);
				if (result == null) result = caseTest(smCubesRegFunctionalityTest);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CocamoPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE: {
				SMCubesRegFunctionalityTestModule smCubesRegFunctionalityTestModule = (SMCubesRegFunctionalityTestModule)theEObject;
				T result = caseSMCubesRegFunctionalityTestModule(smCubesRegFunctionalityTestModule);
				if (result == null) result = caseTestModule(smCubesRegFunctionalityTestModule);
				if (result == null) result = caseModule(smCubesRegFunctionalityTestModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Static Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Static Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesStaticModel(SMCubesStaticModel object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Test Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Test Input Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesTestInputData(SMCubesTestInputData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Report Results</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Report Results</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesReportResults(SMCubesReportResults object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes BDD Test</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes BDD Test</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesBDDTest(SMCubesBDDTest object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes BDD Test Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes BDD Test Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesBDDTestModule(SMCubesBDDTestModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Static Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Static Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseStaticModel(StaticModel object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Reg Functionality Test</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Reg Functionality Test</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesRegFunctionalityTest(SMCubesRegFunctionalityTest object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Reg Functionality Test Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Reg Functionality Test Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesRegFunctionalityTestModule(SMCubesRegFunctionalityTestModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Test Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Test Input Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTestInputData(TestInputData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Result</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Result</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseResult(Result object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Test</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Test</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTest(Test object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTest(BDDTest object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTestModule(TestModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reg Functionality Test</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reg Functionality Test</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRegFunctionalityTest(RegFunctionalityTest object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object)
	{
		return null;
	}

} //CocamoSwitch
