/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ExclusiveGateway;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Exclusive Gateway</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ExclusiveGatewayTest extends GatewayTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ExclusiveGatewayTest.class);
	}

	/**
	 * Constructs a new Exclusive Gateway test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExclusiveGatewayTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Exclusive Gateway test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ExclusiveGateway getFixture() {
		return (ExclusiveGateway)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Bpmn_liteFactory.eINSTANCE.createExclusiveGateway());
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

} //ExclusiveGatewayTest
