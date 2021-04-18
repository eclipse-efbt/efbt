/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data.util;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.*;

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
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage
 * @generated
 */
public class Test_input_dataSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Test_input_dataPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test_input_dataSwitch()
	{
		if (modelPackage == null) {
			modelPackage = Test_input_dataPackage.eINSTANCE;
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
			case Test_input_dataPackage.INPUT_DATA_MODULE: {
				InputDataModule inputDataModule = (InputDataModule)theEObject;
				T result = caseInputDataModule(inputDataModule);
				if (result == null) result = caseModule(inputDataModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS: {
				UsefulInputDataSnippets usefulInputDataSnippets = (UsefulInputDataSnippets)theEObject;
				T result = caseUsefulInputDataSnippets(usefulInputDataSnippets);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_input_dataPackage.TEST_INPUT_DATA: {
				TestInputData testInputData = (TestInputData)theEObject;
				T result = caseTestInputData(testInputData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_input_dataPackage.EXTERNAL_TEST_DATA: {
				ExternalTestData externalTestData = (ExternalTestData)theEObject;
				T result = caseExternalTestData(externalTestData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE: {
				UsefulInputDataSnippetsModule usefulInputDataSnippetsModule = (UsefulInputDataSnippetsModule)theEObject;
				T result = caseUsefulInputDataSnippetsModule(usefulInputDataSnippetsModule);
				if (result == null) result = caseModule(usefulInputDataSnippetsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Input Data Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Input Data Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseInputDataModule(InputDataModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Useful Input Data Snippets</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Useful Input Data Snippets</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUsefulInputDataSnippets(UsefulInputDataSnippets object)
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
	 * Returns the result of interpreting the object as an instance of '<em>External Test Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>External Test Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseExternalTestData(ExternalTestData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Useful Input Data Snippets Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Useful Input Data Snippets Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUsefulInputDataSnippetsModule(UsefulInputDataSnippetsModule object)
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

} //Test_input_dataSwitch
