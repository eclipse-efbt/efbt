/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test.Test;
import org.eclipse.efbt.cocalimo.core.model.test.TestFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Test</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestTest extends TestCase {

	/**
	 * The fixture for this Test test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Test fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(TestTest.class);
	}

	/**
	 * Constructs a new Test test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Test test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(Test fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Test test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Test getFixture() {
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
		setFixture(TestFactory.eINSTANCE.createTest());
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

} //TestTest
