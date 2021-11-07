/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SequenceFlow;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Sequence Flow</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class SequenceFlowTest extends FlowElementTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(SequenceFlowTest.class);
	}

	/**
	 * Constructs a new Sequence Flow test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SequenceFlowTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Sequence Flow test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected SequenceFlow getFixture() {
		return (SequenceFlow)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Bpmn_liteFactory.eINSTANCE.createSequenceFlow());
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

} //SequenceFlowTest
