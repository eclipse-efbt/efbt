/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.View;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ViewTest extends SQLEntityTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ViewTest.class);
	}

	/**
	 * Constructs a new View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected View getFixture() {
		return (View)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Sql_liteFactory.eINSTANCE.createView());
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

} //ViewTest
