/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Scenario</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ScenarioTest extends TestCase {

	/**
	 * The fixture for this Scenario test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Scenario fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ScenarioTest.class);
	}

	/**
	 * Constructs a new Scenario test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ScenarioTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Scenario test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(Scenario fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Scenario test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Scenario getFixture() {
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
		setFixture(ScenariosFactory.eINSTANCE.createScenario());
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

} //ScenarioTest
