/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestDefinitionTest extends TestCase {

	/**
	 * The fixture for this Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestDefinition fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(TestDefinitionTest.class);
	}

	/**
	 * Constructs a new Test Definition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestDefinitionTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(TestDefinition fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestDefinition getFixture() {
		return fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createTestDefinition());
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

} //TestDefinitionTest
