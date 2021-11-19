/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Convert SQL Developer Model To Ecore</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ConvertSQLDeveloperModelToEcoreTest extends PlatformCallTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ConvertSQLDeveloperModelToEcoreTest.class);
	}

	/**
	 * Constructs a new Convert SQL Developer Model To Ecore test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ConvertSQLDeveloperModelToEcoreTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Convert SQL Developer Model To Ecore test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ConvertSQLDeveloperModelToEcore getFixture() {
		return (ConvertSQLDeveloperModelToEcore)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Platform_callFactory.eINSTANCE.createConvertSQLDeveloperModelToEcore());
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

} //ConvertSQLDeveloperModelToEcoreTest
