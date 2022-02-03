/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.tests;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

/**
 * <!-- begin-user-doc -->
 * A test suite for the '<em><b>ecore_plus_structure</b></em>' package.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_structureTests extends TestSuite {

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
		TestSuite suite = new Ecore_plus_structureTests("ecore_plus_structure Tests");
		suite.addTestSuite(BaseEntityTest.class);
		suite.addTestSuite(HeirarchicalEnumTest.class);
		suite.addTestSuite(HeirarchicalEnumLiteralTest.class);
		suite.addTestSuite(EnumeratedDomainTest.class);
		suite.addTestSuite(SubDomainEnumTest.class);
		suite.addTestSuite(EntityTest.class);
		suite.addTestSuite(BaseEntityFeatureTest.class);
		return suite;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structureTests(String name) {
		super(name);
	}

} //Ecore_plus_structureTests
