/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>BDD Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class BDDTestDefinitionTest extends TestDefinitionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(BDDTestDefinitionTest.class);
	}

	/**
	 * Constructs a new BDD Test Definition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestDefinitionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this BDD Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected BDDTestDefinition getFixture() {
		return (BDDTestDefinition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createBDDTestDefinition());
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

} //BDDTestDefinitionTest
