/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Platform Call</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class PlatformCallTest extends TestCase {

	/**
	 * The fixture for this Platform Call test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCall fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(PlatformCallTest.class);
	}

	/**
	 * Constructs a new Platform Call test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PlatformCallTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Platform Call test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(PlatformCall fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Platform Call test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCall getFixture() {
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
		setFixture(Platform_callFactory.eINSTANCE.createPlatformCall());
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

} //PlatformCallTest
