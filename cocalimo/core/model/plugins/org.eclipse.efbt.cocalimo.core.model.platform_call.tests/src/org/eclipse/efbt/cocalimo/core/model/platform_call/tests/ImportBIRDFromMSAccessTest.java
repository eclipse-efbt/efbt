/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Import BIRD From MS Access</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ImportBIRDFromMSAccessTest extends PlatformCallTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ImportBIRDFromMSAccessTest.class);
	}

	/**
	 * Constructs a new Import BIRD From MS Access test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ImportBIRDFromMSAccessTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Import BIRD From MS Access test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ImportBIRDFromMSAccess getFixture() {
		return (ImportBIRDFromMSAccess)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Platform_callFactory.eINSTANCE.createImportBIRDFromMSAccess());
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

} //ImportBIRDFromMSAccessTest
