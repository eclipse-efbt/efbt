/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Union Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class UnionRowCreationApproachTest extends RowCreationApproachTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(UnionRowCreationApproachTest.class);
	}

	/**
	 * Constructs a new Union Row Creation Approach test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnionRowCreationApproachTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Union Row Creation Approach test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected UnionRowCreationApproach getFixture() {
		return (UnionRowCreationApproach)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createUnionRowCreationApproach());
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

} //UnionRowCreationApproachTest
