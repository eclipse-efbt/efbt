/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>BDD Test Template</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class BDDTestTemplateTest extends TestCase {

	/**
	 * The fixture for this BDD Test Template test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestTemplate fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(BDDTestTemplateTest.class);
	}

	/**
	 * Constructs a new BDD Test Template test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestTemplateTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this BDD Test Template test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(BDDTestTemplate fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this BDD Test Template test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestTemplate getFixture() {
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
		setFixture(Test_definitionFactory.eINSTANCE.createBDDTestTemplate());
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

} //BDDTestTemplateTest
