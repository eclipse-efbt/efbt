/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.util;

import org.eclipse.efbt.cocalimo.core.model.test_definition.*;

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
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage
 * @generated
 */
public class Test_definitionSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Test_definitionPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test_definitionSwitch()
	{
		if (modelPackage == null) {
			modelPackage = Test_definitionPackage.eINSTANCE;
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS: {
				BDDTestContraints bddTestContraints = (BDDTestContraints)theEObject;
				T result = caseBDDTestContraints(bddTestContraints);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.BDD_TEST_TEMPLATE_MODULE: {
				BDDTestTemplateModule bddTestTemplateModule = (BDDTestTemplateModule)theEObject;
				T result = caseBDDTestTemplateModule(bddTestTemplateModule);
				if (result == null) result = caseModule(bddTestTemplateModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.BDD_TEST_TEMPLATE: {
				BDDTestTemplate bddTestTemplate = (BDDTestTemplate)theEObject;
				T result = caseBDDTestTemplate(bddTestTemplate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE: {
				BDDTestConstraintsModule bddTestConstraintsModule = (BDDTestConstraintsModule)theEObject;
				T result = caseBDDTestConstraintsModule(bddTestConstraintsModule);
				if (result == null) result = caseModule(bddTestConstraintsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.BDD_TEST_DEFINITION: {
				BDDTestDefinition bddTestDefinition = (BDDTestDefinition)theEObject;
				T result = caseBDDTestDefinition(bddTestDefinition);
				if (result == null) result = caseTestDefinition(bddTestDefinition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.E2EBDD_TEST_DEFINITION: {
				E2EBDDTestDefinition e2EBDDTestDefinition = (E2EBDDTestDefinition)theEObject;
				T result = caseE2EBDDTestDefinition(e2EBDDTestDefinition);
				if (result == null) result = caseBDDTestDefinition(e2EBDDTestDefinition);
				if (result == null) result = caseTestDefinition(e2EBDDTestDefinition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.UNIT_TEST_DEFINITION: {
				UnitTestDefinition unitTestDefinition = (UnitTestDefinition)theEObject;
				T result = caseUnitTestDefinition(unitTestDefinition);
				if (result == null) result = caseBDDTestDefinition(unitTestDefinition);
				if (result == null) result = caseTestDefinition(unitTestDefinition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.CLAUSE: {
				Clause clause = (Clause)theEObject;
				T result = caseClause(clause);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.PARAM: {
				Param param = (Param)theEObject;
				T result = caseParam(param);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.GIVEN: {
				Given given = (Given)theEObject;
				T result = caseGiven(given);
				if (result == null) result = caseClause(given);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.WHEN: {
				When when = (When)theEObject;
				T result = caseWhen(when);
				if (result == null) result = caseClause(when);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.THEN: {
				Then then = (Then)theEObject;
				T result = caseThen(then);
				if (result == null) result = caseClause(then);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.CLAUSE_TEXT: {
				ClauseText clauseText = (ClauseText)theEObject;
				T result = caseClauseText(clauseText);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.COVERAGE_GRID: {
				CoverageGrid coverageGrid = (CoverageGrid)theEObject;
				T result = caseCoverageGrid(coverageGrid);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.COVERAGE_GRID_POINT: {
				CoverageGridPoint coverageGridPoint = (CoverageGridPoint)theEObject;
				T result = caseCoverageGridPoint(coverageGridPoint);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.BDD_TEST_DEFINITION_MODULE: {
				BDDTestDefinitionModule bddTestDefinitionModule = (BDDTestDefinitionModule)theEObject;
				T result = caseBDDTestDefinitionModule(bddTestDefinitionModule);
				if (result == null) result = caseModule(bddTestDefinitionModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.TEST_DEFINITION: {
				TestDefinition testDefinition = (TestDefinition)theEObject;
				T result = caseTestDefinition(testDefinition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION: {
				RegFunctionalityTestDefinition regFunctionalityTestDefinition = (RegFunctionalityTestDefinition)theEObject;
				T result = caseRegFunctionalityTestDefinition(regFunctionalityTestDefinition);
				if (result == null) result = caseTestDefinition(regFunctionalityTestDefinition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE: {
				RegFunctionalityTestDefinitionModule regFunctionalityTestDefinitionModule = (RegFunctionalityTestDefinitionModule)theEObject;
				T result = caseRegFunctionalityTestDefinitionModule(regFunctionalityTestDefinitionModule);
				if (result == null) result = caseModule(regFunctionalityTestDefinitionModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Contraints</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Contraints</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestContraints(BDDTestContraints object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Template Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Template Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestTemplateModule(BDDTestTemplateModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Template</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Template</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestTemplate(BDDTestTemplate object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Constraints Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Constraints Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestConstraintsModule(BDDTestConstraintsModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Definition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Definition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestDefinition(BDDTestDefinition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>E2EBDD Test Definition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>E2EBDD Test Definition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseE2EBDDTestDefinition(E2EBDDTestDefinition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Unit Test Definition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Unit Test Definition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUnitTestDefinition(UnitTestDefinition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Clause</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Clause</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseClause(Clause object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Param</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Param</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParam(Param object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Given</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Given</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGiven(Given object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>When</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>When</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseWhen(When object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Then</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Then</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseThen(Then object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Clause Text</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Clause Text</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseClauseText(ClauseText object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Coverage Grid</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Coverage Grid</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCoverageGrid(CoverageGrid object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Coverage Grid Point</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Coverage Grid Point</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCoverageGridPoint(CoverageGridPoint object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BDD Test Definition Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BDD Test Definition Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBDDTestDefinitionModule(BDDTestDefinitionModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Test Definition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Test Definition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTestDefinition(TestDefinition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reg Functionality Test Definition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reg Functionality Test Definition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRegFunctionalityTestDefinition(RegFunctionalityTestDefinition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reg Functionality Test Definition Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reg Functionality Test Definition Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRegFunctionalityTestDefinitionModule(RegFunctionalityTestDefinitionModule object)
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

} //Test_definitionSwitch
