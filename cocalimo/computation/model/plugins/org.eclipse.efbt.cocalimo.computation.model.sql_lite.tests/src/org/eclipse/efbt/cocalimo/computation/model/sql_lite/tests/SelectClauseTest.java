/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectClause;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_liteFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Select Clause</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class SelectClauseTest extends TestCase {

	/**
	 * The fixture for this Select Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectClause fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(SelectClauseTest.class);
	}

	/**
	 * Constructs a new Select Clause test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectClauseTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Select Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(SelectClause fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Select Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectClause getFixture() {
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
		setFixture(Sql_liteFactory.eINSTANCE.createSelectClause());
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

} //SelectClauseTest