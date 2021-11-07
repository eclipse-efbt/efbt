/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ParallelGateway;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Parallel Gateway</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ParallelGatewayTest extends GatewayTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ParallelGatewayTest.class);
	}

	/**
	 * Constructs a new Parallel Gateway test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ParallelGatewayTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Parallel Gateway test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ParallelGateway getFixture() {
		return (ParallelGateway)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Bpmn_liteFactory.eINSTANCE.createParallelGateway());
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

} //ParallelGatewayTest
