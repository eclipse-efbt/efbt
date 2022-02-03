/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SQLEntity;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>SQL Entity</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class SQLEntityTest extends TestCase {

	/**
	 * The fixture for this SQL Entity test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SQLEntity fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(SQLEntityTest.class);
	}

	/**
	 * Constructs a new SQL Entity test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntityTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this SQL Entity test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(SQLEntity fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this SQL Entity test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SQLEntity getFixture() {
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
		setFixture(Sql_liteFactory.eINSTANCE.createSQLEntity());
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

} //SQLEntityTest
