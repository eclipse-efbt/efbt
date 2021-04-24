/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;
import org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Unit Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class UnitTestDefinitionTest extends BDDTestDefinitionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(UnitTestDefinitionTest.class);
	}

	/**
	 * Constructs a new Unit Test Definition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnitTestDefinitionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Unit Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected UnitTestDefinition getFixture() {
		return (UnitTestDefinition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createUnitTestDefinition());
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

} //UnitTestDefinitionTest
