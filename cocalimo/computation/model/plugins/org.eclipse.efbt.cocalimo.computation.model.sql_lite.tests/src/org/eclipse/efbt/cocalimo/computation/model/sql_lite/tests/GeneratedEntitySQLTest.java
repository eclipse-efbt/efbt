/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.GeneratedEntitySQL;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Generated Entity SQL</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class GeneratedEntitySQLTest extends TestCase {

	/**
	 * The fixture for this Generated Entity SQL test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GeneratedEntitySQL fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(GeneratedEntitySQLTest.class);
	}

	/**
	 * Constructs a new Generated Entity SQL test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntitySQLTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Generated Entity SQL test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(GeneratedEntitySQL fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Generated Entity SQL test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GeneratedEntitySQL getFixture() {
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
		setFixture(Sql_liteFactory.eINSTANCE.createGeneratedEntitySQL());
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

} //GeneratedEntitySQLTest
