/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test.BDDTest;
import org.eclipse.efbt.cocalimo.core.model.test.TestFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>BDD Test</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class BDDTestTest extends TestTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(BDDTestTest.class);
	}

	/**
	 * Constructs a new BDD Test test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this BDD Test test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected BDDTest getFixture() {
		return (BDDTest)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(TestFactory.eINSTANCE.createBDDTest());
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

} //BDDTestTest
