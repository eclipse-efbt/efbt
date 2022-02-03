/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SQLEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>SQL Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class SQLEntityModuleTest extends TestCase {

	/**
	 * The fixture for this SQL Entity Module test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SQLEntityModule fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(SQLEntityModuleTest.class);
	}

	/**
	 * Constructs a new SQL Entity Module test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntityModuleTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this SQL Entity Module test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(SQLEntityModule fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this SQL Entity Module test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SQLEntityModule getFixture() {
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
		setFixture(Sql_liteFactory.eINSTANCE.createSQLEntityModule());
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

} //SQLEntityModuleTest
