/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.*;

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
public class Test_definitionFactoryImpl extends EFactoryImpl implements Test_definitionFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Test_definitionFactory init()
	{
		try {
			Test_definitionFactory theTest_definitionFactory = (Test_definitionFactory)EPackage.Registry.INSTANCE.getEFactory(Test_definitionPackage.eNS_URI);
			if (theTest_definitionFactory != null) {
				return theTest_definitionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Test_definitionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test_definitionFactoryImpl()
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS: return createBDDTestContraints();
			case Test_definitionPackage.BDD_TEST_TEMPLATE_MODULE: return createBDDTestTemplateModule();
			case Test_definitionPackage.BDD_TEST_TEMPLATE: return createBDDTestTemplate();
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE: return createBDDTestConstraintsModule();
			case Test_definitionPackage.BDD_TEST_DEFINITION: return createBDDTestDefinition();
			case Test_definitionPackage.E2EBDD_TEST_DEFINITION: return createE2EBDDTestDefinition();
			case Test_definitionPackage.UNIT_TEST_DEFINITION: return createUnitTestDefinition();
			case Test_definitionPackage.CLAUSE: return createClause();
			case Test_definitionPackage.PARAM: return createParam();
			case Test_definitionPackage.GIVEN: return createGiven();
			case Test_definitionPackage.WHEN: return createWhen();
			case Test_definitionPackage.THEN: return createThen();
			case Test_definitionPackage.CLAUSE_TEXT: return createClauseText();
			case Test_definitionPackage.COVERAGE_GRID: return createCoverageGrid();
			case Test_definitionPackage.COVERAGE_GRID_POINT: return createCoverageGridPoint();
			case Test_definitionPackage.BDD_TEST_DEFINITION_MODULE: return createBDDTestDefinitionModule();
			case Test_definitionPackage.TEST_DEFINITION: return createTestDefinition();
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION: return createRegFunctionalityTestDefinition();
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE: return createRegFunctionalityTestDefinitionModule();
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
	public BDDTestContraints createBDDTestContraints()
	{
		BDDTestContraintsImpl bddTestContraints = new BDDTestContraintsImpl();
		return bddTestContraints;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestTemplateModule createBDDTestTemplateModule()
	{
		BDDTestTemplateModuleImpl bddTestTemplateModule = new BDDTestTemplateModuleImpl();
		return bddTestTemplateModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestTemplate createBDDTestTemplate()
	{
		BDDTestTemplateImpl bddTestTemplate = new BDDTestTemplateImpl();
		return bddTestTemplate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestConstraintsModule createBDDTestConstraintsModule()
	{
		BDDTestConstraintsModuleImpl bddTestConstraintsModule = new BDDTestConstraintsModuleImpl();
		return bddTestConstraintsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestDefinition createBDDTestDefinition()
	{
		BDDTestDefinitionImpl bddTestDefinition = new BDDTestDefinitionImpl();
		return bddTestDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public E2EBDDTestDefinition createE2EBDDTestDefinition()
	{
		E2EBDDTestDefinitionImpl e2EBDDTestDefinition = new E2EBDDTestDefinitionImpl();
		return e2EBDDTestDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public UnitTestDefinition createUnitTestDefinition()
	{
		UnitTestDefinitionImpl unitTestDefinition = new UnitTestDefinitionImpl();
		return unitTestDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Clause createClause()
	{
		ClauseImpl clause = new ClauseImpl();
		return clause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Param createParam()
	{
		ParamImpl param = new ParamImpl();
		return param;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Given createGiven()
	{
		GivenImpl given = new GivenImpl();
		return given;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public When createWhen()
	{
		WhenImpl when = new WhenImpl();
		return when;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Then createThen()
	{
		ThenImpl then = new ThenImpl();
		return then;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ClauseText createClauseText()
	{
		ClauseTextImpl clauseText = new ClauseTextImpl();
		return clauseText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CoverageGrid createCoverageGrid()
	{
		CoverageGridImpl coverageGrid = new CoverageGridImpl();
		return coverageGrid;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CoverageGridPoint createCoverageGridPoint()
	{
		CoverageGridPointImpl coverageGridPoint = new CoverageGridPointImpl();
		return coverageGridPoint;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestDefinitionModule createBDDTestDefinitionModule()
	{
		BDDTestDefinitionModuleImpl bddTestDefinitionModule = new BDDTestDefinitionModuleImpl();
		return bddTestDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestDefinition createTestDefinition()
	{
		TestDefinitionImpl testDefinition = new TestDefinitionImpl();
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTestDefinition createRegFunctionalityTestDefinition()
	{
		RegFunctionalityTestDefinitionImpl regFunctionalityTestDefinition = new RegFunctionalityTestDefinitionImpl();
		return regFunctionalityTestDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTestDefinitionModule createRegFunctionalityTestDefinitionModule()
	{
		RegFunctionalityTestDefinitionModuleImpl regFunctionalityTestDefinitionModule = new RegFunctionalityTestDefinitionModuleImpl();
		return regFunctionalityTestDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test_definitionPackage getTest_definitionPackage()
	{
		return (Test_definitionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Test_definitionPackage getPackage()
	{
		return Test_definitionPackage.eINSTANCE;
	}

} //Test_definitionFactoryImpl
