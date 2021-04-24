/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>E2EBDD Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class E2EBDDTestDefinitionTest extends BDDTestDefinitionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(E2EBDDTestDefinitionTest.class);
	}

	/**
	 * Constructs a new E2EBDD Test Definition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public E2EBDDTestDefinitionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this E2EBDD Test Definition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected E2EBDDTestDefinition getFixture() {
		return (E2EBDDTestDefinition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createE2EBDDTestDefinition());
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

} //E2EBDDTestDefinitionTest
