/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Aggregate Derived Feature</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class AggregateDerivedFeatureTest extends DerivedFeatureTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(AggregateDerivedFeatureTest.class);
	}

	/**
	 * Constructs a new Aggregate Derived Feature test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateDerivedFeatureTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Aggregate Derived Feature test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected AggregateDerivedFeature getFixture() {
		return (AggregateDerivedFeature)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createAggregateDerivedFeature());
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

} //AggregateDerivedFeatureTest
