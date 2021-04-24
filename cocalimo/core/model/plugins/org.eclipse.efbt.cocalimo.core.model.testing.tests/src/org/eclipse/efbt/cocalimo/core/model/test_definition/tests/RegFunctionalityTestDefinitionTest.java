/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Reg Functionality Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class RegFunctionalityTestDefinitionTest extends TestDefinitionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(RegFunctionalityTestDefinitionTest.class);
	}

	/**
	 * Constructs a new Reg Functionality Test Definition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RegFunctionalityTestDefinitionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Reg Functionality Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected RegFunctionalityTestDefinition getFixture() {
		return (RegFunctionalityTestDefinition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createRegFunctionalityTestDefinition());
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#tearDown()
	 * @generated
	 */
	@Override
	protected void tearDown() throws Exception {
		setFixture(null);
	}

} //RegFunctionalityTestDefinitionTest
