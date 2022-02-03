/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Member Parameter</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class MemberParameterTest extends ParameterTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(MemberParameterTest.class);
	}

	/**
	 * Constructs a new Member Parameter test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberParameterTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Member Parameter test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected MemberParameter getFixture() {
		return (MemberParameter)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createMemberParameter());
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

} //MemberParameterTest
