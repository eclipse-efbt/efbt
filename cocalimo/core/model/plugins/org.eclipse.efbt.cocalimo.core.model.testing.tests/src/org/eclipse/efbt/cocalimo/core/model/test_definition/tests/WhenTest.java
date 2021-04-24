/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;
import org.eclipse.efbt.cocalimo.core.model.test_definition.When;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>When</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class WhenTest extends ClauseTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(WhenTest.class);
	}

	/**
	 * Constructs a new When test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public WhenTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this When test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected When getFixture() {
		return (When)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Test_definitionFactory.eINSTANCE.createWhen());
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

} //WhenTest
