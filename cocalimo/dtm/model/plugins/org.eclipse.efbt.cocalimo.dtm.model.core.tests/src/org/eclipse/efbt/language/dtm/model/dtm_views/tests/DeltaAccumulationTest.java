/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.DeltaAccumulation;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Delta Accumulation</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class DeltaAccumulationTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(DeltaAccumulationTest.class);
	}

	/**
	 * Constructs a new Delta Accumulation test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DeltaAccumulationTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Delta Accumulation test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected DeltaAccumulation getFixture() {
		return (DeltaAccumulation)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createDeltaAccumulation());
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

} //DeltaAccumulationTest
