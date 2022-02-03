/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Get Attribute From Entity Derived Feature</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class GetAttributeFromEntityDerivedFeatureTest extends DerivedFeatureTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(GetAttributeFromEntityDerivedFeatureTest.class);
	}

	/**
	 * Constructs a new Get Attribute From Entity Derived Feature test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GetAttributeFromEntityDerivedFeatureTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Get Attribute From Entity Derived Feature test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected GetAttributeFromEntityDerivedFeature getFixture() {
		return (GetAttributeFromEntityDerivedFeature)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createGetAttributeFromEntityDerivedFeature());
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

} //GetAttributeFromEntityDerivedFeatureTest
