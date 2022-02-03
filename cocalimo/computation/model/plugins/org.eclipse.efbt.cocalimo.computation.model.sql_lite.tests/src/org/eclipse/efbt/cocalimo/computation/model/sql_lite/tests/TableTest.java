/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Table;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Table</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class TableTest extends SQLEntityTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(TableTest.class);
	}

	/**
	 * Constructs a new Table test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TableTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Table test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Table getFixture() {
		return (Table)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Sql_liteFactory.eINSTANCE.createTable());
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

} //TableTest
