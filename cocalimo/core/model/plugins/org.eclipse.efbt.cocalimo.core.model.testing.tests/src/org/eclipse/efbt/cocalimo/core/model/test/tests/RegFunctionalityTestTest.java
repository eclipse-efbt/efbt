/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest;
import org.eclipse.efbt.cocalimo.core.model.test.TestFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Reg Functionality Test</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class RegFunctionalityTestTest extends TestTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(RegFunctionalityTestTest.class);
	}

	/**
	 * Constructs a new Reg Functionality Test test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RegFunctionalityTestTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Reg Functionality Test test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected RegFunctionalityTest getFixture() {
		return (RegFunctionalityTest)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(TestFactory.eINSTANCE.createRegFunctionalityTest());
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

} //RegFunctionalityTestTest
