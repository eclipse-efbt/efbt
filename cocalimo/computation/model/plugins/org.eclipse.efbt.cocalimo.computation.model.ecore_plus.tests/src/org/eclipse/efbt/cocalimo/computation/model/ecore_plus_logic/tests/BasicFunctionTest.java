/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Basic Function</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class BasicFunctionTest extends FunctionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(BasicFunctionTest.class);
	}

	/**
	 * Constructs a new Basic Function test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunctionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Basic Function test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected BasicFunction getFixture() {
		return (BasicFunction)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createBasicFunction());
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

} //BasicFunctionTest
