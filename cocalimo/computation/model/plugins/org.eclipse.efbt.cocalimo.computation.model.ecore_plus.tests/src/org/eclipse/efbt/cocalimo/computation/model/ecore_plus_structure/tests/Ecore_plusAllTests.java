/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.tests;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests.Ecore_plus_logicTests;

/**
 * <!-- begin-user-doc -->
 * A test suite for the '<em><b>Ecore_plus</b></em>' model.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plusAllTests extends TestSuite {

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
		TestSuite suite = new Ecore_plusAllTests("Ecore_plus Tests");
		suite.addTest(Ecore_plus_structureTests.suite());
		suite.addTest(Ecore_plus_logicTests.suite());
		return suite;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plusAllTests(String name) {
		super(name);
	}

} //Ecore_plusAllTests
