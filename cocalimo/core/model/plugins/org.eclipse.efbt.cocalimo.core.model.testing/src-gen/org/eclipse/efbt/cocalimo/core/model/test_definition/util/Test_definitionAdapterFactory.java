/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.util;

import org.eclipse.efbt.cocalimo.core.model.test_definition.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage
 * @generated
 */
public class Test_definitionAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Test_definitionPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Test_definitionAdapterFactory()
	{
		if (modelPackage == null) {
			modelPackage = Test_definitionPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object)
	{
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Test_definitionSwitch<Adapter> modelSwitch =
		new Test_definitionSwitch<Adapter>() {
			@Override
			public Adapter caseBDDTestContraints(BDDTestContraints object) {
				return createBDDTestContraintsAdapter();
			}
			@Override
			public Adapter caseBDDTestTemplateModule(BDDTestTemplateModule object) {
				return createBDDTestTemplateModuleAdapter();
			}
			@Override
			public Adapter caseBDDTestTemplate(BDDTestTemplate object) {
				return createBDDTestTemplateAdapter();
			}
			@Override
			public Adapter caseBDDTestConstraintsModule(BDDTestConstraintsModule object) {
				return createBDDTestConstraintsModuleAdapter();
			}
			@Override
			public Adapter caseBDDTestDefinition(BDDTestDefinition object) {
				return createBDDTestDefinitionAdapter();
			}
			@Override
			public Adapter caseE2EBDDTestDefinition(E2EBDDTestDefinition object) {
				return createE2EBDDTestDefinitionAdapter();
			}
			@Override
			public Adapter caseUnitTestDefinition(UnitTestDefinition object) {
				return createUnitTestDefinitionAdapter();
			}
			@Override
			public Adapter caseClause(Clause object) {
				return createClauseAdapter();
			}
			@Override
			public Adapter caseParam(Param object) {
				return createParamAdapter();
			}
			@Override
			public Adapter caseGiven(Given object) {
				return createGivenAdapter();
			}
			@Override
			public Adapter caseWhen(When object) {
				return createWhenAdapter();
			}
			@Override
			public Adapter caseThen(Then object) {
				return createThenAdapter();
			}
			@Override
			public Adapter caseClauseText(ClauseText object) {
				return createClauseTextAdapter();
			}
			@Override
			public Adapter caseCoverageGrid(CoverageGrid object) {
				return createCoverageGridAdapter();
			}
			@Override
			public Adapter caseCoverageGridPoint(CoverageGridPoint object) {
				return createCoverageGridPointAdapter();
			}
			@Override
			public Adapter caseBDDTestDefinitionModule(BDDTestDefinitionModule object) {
				return createBDDTestDefinitionModuleAdapter();
			}
			@Override
			public Adapter caseTestDefinition(TestDefinition object) {
				return createTestDefinitionAdapter();
			}
			@Override
			public Adapter caseRegFunctionalityTestDefinition(RegFunctionalityTestDefinition object) {
				return createRegFunctionalityTestDefinitionAdapter();
			}
			@Override
			public Adapter caseRegFunctionalityTestDefinitionModule(RegFunctionalityTestDefinitionModule object) {
				return createRegFunctionalityTestDefinitionModuleAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints <em>BDD Test Contraints</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints
	 * @generated
	 */
	public Adapter createBDDTestContraintsAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule <em>BDD Test Template Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule
	 * @generated
	 */
	public Adapter createBDDTestTemplateModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate <em>BDD Test Template</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate
	 * @generated
	 */
	public Adapter createBDDTestTemplateAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule <em>BDD Test Constraints Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule
	 * @generated
	 */
	public Adapter createBDDTestConstraintsModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition <em>BDD Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition
	 * @generated
	 */
	public Adapter createBDDTestDefinitionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition <em>E2EBDD Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition
	 * @generated
	 */
	public Adapter createE2EBDDTestDefinitionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition <em>Unit Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition
	 * @generated
	 */
	public Adapter createUnitTestDefinitionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause <em>Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Clause
	 * @generated
	 */
	public Adapter createClauseAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Param
	 * @generated
	 */
	public Adapter createParamAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Given <em>Given</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Given
	 * @generated
	 */
	public Adapter createGivenAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.When <em>When</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.When
	 * @generated
	 */
	public Adapter createWhenAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Then <em>Then</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Then
	 * @generated
	 */
	public Adapter createThenAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText <em>Clause Text</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText
	 * @generated
	 */
	public Adapter createClauseTextAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid <em>Coverage Grid</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid
	 * @generated
	 */
	public Adapter createCoverageGridAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint <em>Coverage Grid Point</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint
	 * @generated
	 */
	public Adapter createCoverageGridPointAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule <em>BDD Test Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule
	 * @generated
	 */
	public Adapter createBDDTestDefinitionModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition
	 * @generated
	 */
	public Adapter createTestDefinitionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition <em>Reg Functionality Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition
	 * @generated
	 */
	public Adapter createRegFunctionalityTestDefinitionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule <em>Reg Functionality Test Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule
	 * @generated
	 */
	public Adapter createRegFunctionalityTestDefinitionModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //Test_definitionAdapterFactory
