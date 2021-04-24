/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Clause;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Clause</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ClauseTest extends TestCase {

	/**
	 * The fixture for this Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Clause fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ClauseTest.class);
	}

	/**
	 * Constructs a new Clause test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClauseTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(Clause fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Clause test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Clause getFixture() {
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
		setFixture(Test_definitionFactory.eINSTANCE.createClause());
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

} //ClauseTest
