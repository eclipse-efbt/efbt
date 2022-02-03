/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

/**
 * <!-- begin-user-doc -->
 * A test suite for the '<em><b>ecore_plus_logic</b></em>' package.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_logicTests extends TestSuite {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(suite());
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Test suite() {
		TestSuite suite = new Ecore_plus_logicTests("ecore_plus_logic Tests");
		suite.addTestSuite(DerivedEntityTest.class);
		suite.addTestSuite(IntermediateDerivedEntityTest.class);
		suite.addTestSuite(GeneratedEntityTest.class);
		suite.addTestSuite(AggregateDerivedFeatureTest.class);
		suite.addTestSuite(BasicDerivedFeatureTest.class);
		suite.addTestSuite(DerivedFeatureTest.class);
		suite.addTestSuite(BaseFeatureTest.class);
		suite.addTestSuite(GetAttributeFromEntityDerivedFeatureTest.class);
		suite.addTestSuite(AttributeFromEntityParameterTest.class);
		return suite;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicTests(String name) {
		super(name);
	}

} //Ecore_plus_logicTests
