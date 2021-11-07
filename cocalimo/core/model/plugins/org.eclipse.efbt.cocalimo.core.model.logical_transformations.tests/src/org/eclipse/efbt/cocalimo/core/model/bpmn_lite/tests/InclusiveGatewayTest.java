/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.InclusiveGateway;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Inclusive Gateway</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class InclusiveGatewayTest extends GatewayTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(InclusiveGatewayTest.class);
	}

	/**
	 * Constructs a new Inclusive Gateway test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InclusiveGatewayTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Inclusive Gateway test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected InclusiveGateway getFixture() {
		return (InclusiveGateway)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Bpmn_liteFactory.eINSTANCE.createInclusiveGateway());
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

} //InclusiveGatewayTest
