/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.FromClause;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>From Clause</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class FromClauseTest extends TestCase {

	/**
	 * The fixture for this From Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FromClause fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(FromClauseTest.class);
	}

	/**
	 * Constructs a new From Clause test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FromClauseTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this From Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(FromClause fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this From Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FromClause getFixture() {
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
		setFixture(Sql_liteFactory.eINSTANCE.createFromClause());
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

} //FromClauseTest
